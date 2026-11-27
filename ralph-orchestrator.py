#!/usr/bin/env python3
"""Outer trigger-loop that runs a triage/dispatch instruction with Claude (opus).

Multi-repo: each pass runs one `claude -p` opus iteration per configured project.
A pass fires on ANY of three triggers:
  1. a GitHub issue/PR was updated (open, not labeled ultra-ralph) in any repo
  2. a spawned sub-agent/process finished  (it touches $RALPH_WAKE on exit)
  3. one hour elapsed since the last run

Projects come from a config file (default ~/.ralph-orchestrator/projects.json) or,
for a single repo, from command-line flags. Per-project: repo, project number +
node ids, local repo path, worktrees base, and optional `notes` injected into the
prompt. Everything else (OTEL endpoint, ralph.sh path, status pipeline) is global.

ponytail: poll-based, one feature-pass per project per iteration, single global wake
file. Upgrade path: swap the gh poll for a webhook listener if 60s latency matters.
"""
import argparse, atexit, json, os, re, signal, subprocess, sys, time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


MODEL    = "opus"
IGNORE   = "ultra-ralph"            # skip issues/PRs with this label, this session
PRODUCT_LABEL = "product-approved"  # stamped when the product phase is agreed with the reporter
AUTO_LABEL = "agent-approved"       # on an auto_approve repo: counts as approval AND licenses auto-merge
HERE = Path(__file__).resolve().parent            # scripts ship next to this file — portable across hosts
RALPH_SH = str(HERE / "ralph.sh")
RALPH_CLAUDE = str(HERE / "ralph-claude.sh")      # wrapper: always tags usage_mode=ralph
OTEL_ENDPOINT = "http://100.109.196.108:4317"

POLL        = 60                   # seconds between GitHub polls
MAX_WAIT    = 3600                 # 1 hour hard trigger
CONCURRENCY = 3                    # max features in flight per project (one worktree each)
STATE       = Path.home() / ".ralph-orchestrator"
WAKE        = STATE / "wake"
LOCK        = STATE / "orchestrator.lock"
STATUS_MD   = STATE / "status.md"                  # `watch cat ~/.ralph-orchestrator/status.md`
CONFIG      = STATE / "projects.json"


def log(msg):
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}  {msg}", flush=True)


# ── project config ────────────────────────────────────────────────────────────
def _norm_repo(repo, val):
    """Normalize a repo spec: a bare path string or {path, worktrees} -> {path, worktrees, owner}."""
    spec = {"path": val} if isinstance(val, str) else dict(val)
    if not spec.get("path"):
        sys.exit(f"repo {repo} missing local path")
    # expand ~: these go to `git -C` (no shell), where a literal ~ silently yields no worktrees,
    # which reads as "no executor is live" in the board scan
    spec["path"] = os.path.expanduser(spec["path"])
    if spec.get("worktrees"):
        spec["worktrees"] = os.path.expanduser(spec["worktrees"])
    spec.setdefault("worktrees", str(Path(spec["path"]).parent / (Path(spec["path"]).name + "-wt")))
    spec["owner"] = repo.split("/")[0]
    spec["auto_approve"] = bool(spec.get("auto_approve"))   # opt-in: AUTO_LABEL approves + merges
    spec.setdefault("default_branch", "master")
    return spec


def normalize(p):
    """Canonicalize a project: one Project (board) may span several repos.

    Accepts `repos: {owner/name: path | {path, worktrees}}`, or the single-repo
    shorthand `repo` + `path` (+ optional `worktrees`). `owner` is the Project
    owner (defaults to the first repo's owner; override for org-owned boards)."""
    for k in ("project_number", "project_id", "status_field_id"):
        if not p.get(k):
            sys.exit(f"project config missing {k}: {p}")
    if p.get("repo"):                                  # single-repo shorthand -> repos map
        spec = {"path": p.get("path")}
        if p.get("worktrees"):
            spec["worktrees"] = p["worktrees"]
        p.setdefault("repos", {})[p["repo"]] = spec
    if not p.get("repos"):
        sys.exit(f"project {p.get('project_number')} has no repos")
    p["repos"] = {r: _norm_repo(r, v) for r, v in p["repos"].items()}
    p.setdefault("notes", "")
    p["owner"] = p.get("owner") or next(iter(p["repos"])).split("/")[0]
    return p


def load_projects(args):
    if args.repo:                                  # single-repo via flags, ignore config file
        return [normalize({
            "repo": args.repo, "project_number": args.project, "project_id": args.project_id,
            "status_field_id": args.status_field, "path": args.path,
            "worktrees": args.worktrees, "notes": args.notes or "",
        })]
    cfg = json.loads(Path(args.config).read_text())
    return [normalize(p) for p in cfg["projects"]]


# ── per-project gh helpers ─────────────────────────────────────────────────────
def status_opts(p):
    """Live {status name: option id} for the Status field — never hardcode (ids change on rename)."""
    d = _json(["gh", "project", "field-list", str(p["project_number"]), "--owner", p["owner"], "--format", "json"])
    fields = d.get("fields", []) if isinstance(d, dict) else d
    status = next((f for f in fields if f.get("id") == p["status_field_id"]), None)
    if status is None:  # transient gh hiccup (empty/partial fields) — don't crash the whole loop
        raise RuntimeError(f"Status field {p['status_field_id']} not in field-list for project "
                           f"#{p['project_number']} ({len(fields)} fields returned)")
    return {o["name"]: o["id"] for o in status.get("options", [])}


def project_items(p):
    """Issue items on the project, excluding the ignore-labeled ones."""
    d = _json(["gh", "project", "item-list", str(p["project_number"]), "--owner", p["owner"],
               "--format", "json", "--limit", "200"])
    items = d.get("items", []) if isinstance(d, dict) else d
    return [it for it in items
            if it.get("content", {}).get("type") == "Issue"
            and IGNORE not in (it.get("labels") or [])]


# ── instruction ────────────────────────────────────────────────────────────────
def build_instruction(p, opts):
    optline = ", ".join(f"{k}={v}" for k, v in opts.items())
    owner, num = p["owner"], p["project_number"]
    pid, fid = p["project_id"], p["status_field_id"]
    notes = f"\n   - Project-specific notes: {p['notes']}" if p.get("notes") else ""
    repolines = "\n".join(f"  - {r}: local clone {spec['path']}, worktrees under {spec['worktrees']}"
                          + (f", default branch `{spec['default_branch']}` (use it wherever this "
                             f"text says master)" if spec["default_branch"] != "master" else "")
                          + (" [AUTO-APPROVE]" if spec["auto_approve"] else "")
                          for r, spec in p["repos"].items())
    auto = [r for r, spec in p["repos"].items() if spec["auto_approve"]]
    autoblock = "" if not auto else f"""

AUTO-APPROVE repos ({", ".join(auto)}) — these rules OVERRIDE the human gates above, for items in
those repos ONLY. Never apply `{AUTO_LABEL}` to, or auto-merge in, any other repo.
  ESCALATION TRIGGERS — an item or PR that touches ANY of these goes to `Ready For Human` with a
  comment naming the trigger, and is never auto-approved or auto-merged: a migration that drops or
  rewrites existing data; auth, secrets, or internet exposure; the Claude call budget/concurrency;
  a new external service or paid dependency; a product decision not covered by the agreed brief; a
  major design dilemma (two reasonable approaches with lasting consequences); a slice that is L/XL
  and cannot be split. Also follow the repo's CLAUDE.md / docs/PLAN.md guidelines.
  - PRODUCT phase: post the Product Brief as usual, then — if no trigger applies — confirm it
    yourself: stamp `{PRODUCT_LABEL}` and continue straight into the TECHNICAL phase in the same pass
    (do not wait in `Needs Info`). If the brief needs the reporter's input, behave as normal.
  - TECHNICAL phase: after the Technical Plan, if the item is XS/S/M and no trigger applies, add the
    `{AUTO_LABEL}` label (create it if missing: `gh label create {AUTO_LABEL} -R <repo> --color
    5319E7 --description "approved by the orchestrator under the repo guidelines" 2>/dev/null || true`)
    and set `Ready For Agent`. `{AUTO_LABEL}` on a `Ready For Agent` item counts as the human
    "approved" comment for dispatch.
  - Dispatch on an `{AUTO_LABEL}` item honours "Blocked by #n" lines in the issue body: do not
    dispatch while any blocker is still open.
  - MERGE: every executor for an `{AUTO_LABEL}` item gets this appended to its prompt: "After the PR
    is open and CI is green, run /code-review on the PR. Fix every important finding (/tdd), push,
    and re-review — at most 2 fix rounds. If the review is clean and no escalation trigger applies,
    merge with `gh pr merge <pr> --squash --delete-branch`, then follow the repo CLAUDE.md
    post-merge verification. Otherwise comment why and set Status `Ready For Human`."
    And on every pass: an `In review` `{AUTO_LABEL}` item whose PR has CI `pass`, is MERGEABLE, and
    has no LIVE executor has a stranded merge step — relaunch a detached executor in its worktree
    with just that review-and-merge instruction. After a merge: set Status `Done`, remove the
    worktree."""
    routing = ("This Project spans MULTIPLE repos. Every project item carries its own repo in "
               "`content.repository` (from item-list) — do ALL git, PR, label, and worktree work in "
               "THAT repo, using its clone/worktrees path below:" if len(p["repos"]) > 1
               else "All items in this Project belong to one repo:")
    return f"""You are the triage+dispatch orchestrator for GitHub Project #{num} (owner {owner}).
Run ONE full pass over the whole board, then exit (an outer loop re-invokes you on the next trigger).

{routing}
{repolines}

Triage state lives in the Project Status field, NOT in labels.
To set a status: `gh project item-edit --id <ITEM_ID> --project-id {pid} --field-id {fid} --single-select-option-id <OPT>`
where <ITEM_ID> is the project item id from `gh project item-list {num} --owner {owner} --format json`
and <OPT> is the option id for the target status (live ids): {optline}.
Status pipeline (use the names exactly as above):
  Backlog (not started) / Needs Triage  -> you triage these
  Ready For Agent (ready to be picked up) -> dispatch when a human approves
  Needs Info / Ready For Human           -> needs a human
  In progress (actively worked on) -> In review (PR open, in review) -> Done (completed)

Hard rule: IGNORE every issue and PR labeled `{IGNORE}` — do not read, triage, or act on them.
Propagate it: EVERY claude process or sub-agent you spawn MUST include this sentence verbatim in
its prompt -> "Ignore anything labeled `{IGNORE}`; never read, modify, or open a PR against it."

Triage is TWO phases: agree on WHAT the feature is with the reporter (product), THEN plan HOW it
fits the codebase (technical). The `{PRODUCT_LABEL}` label marks that the product phase is settled —
never re-open the product discussion on an item that already has it.

1. PRODUCT phase — for items with Status `Backlog` or `Needs Triage` that do NOT have the
   `{PRODUCT_LABEL}` label (use /triage): agree on WHAT the feature is, in plain language, BEFORE any
   technical detail. Post a "Product Brief" comment — Problem / Who it affects / What "done" looks
   like to a user / Out of scope — describe BEHAVIOUR, not implementation (no files, APIs, or
   architecture). For a UI feature, ALSO post a self-contained HTML mockup of the feature inline in
   the comment so the reporter can see and agree on the look and layout before anything is built
   (the mockup is a visual, not implementation — the one allowed exception to "no code"); follow the
   item's repo CLAUDE.md for mockup conventions (e.g. language/RTL). End by asking the reporter to
   confirm or correct. Set Status `Needs Info` (ball in the reporter's court). If it is
   too unclear to even draft one, ask the blocking question and set `Needs Info`. Use `Ready For
   Human` for anything needing a human DECISION rather than reporter input.

2. TECHNICAL phase — for items with Status `Needs Info` where the reporter has replied to a Product
   Brief: if they corrected it, revise the brief and stay `Needs Info`. If they CONFIRMED, then:
   - Stamp the agreement on the item's repo: `gh issue edit <n> -R <item repo> --add-label
     {PRODUCT_LABEL}` (create it first if missing: `gh label create {PRODUCT_LABEL} -R <item repo>
     --color 0E8A16 --description "product requirements agreed with reporter" 2>/dev/null || true`).
   - Post a "Technical Plan" comment: how it integrates into that repo's codebase (affected
     files/layers, API, tests) and, for a UI feature, how it realizes the agreed HTML mockup from the
     Product Brief. Follow the item's repo CLAUDE.md conventions. If too large for one PR, split with
     /to-issues. If it needs coordinated changes across repos, split into one linked issue per repo
     (sequenced by dependency) — never one PR spanning repos.
   - Set Status `Ready For Agent`.{notes}

3. Reconcile, THEN dispatch.
   Branch convention — an item's worktree/branch/PR is ANY branch named `<type>/<n>[-slug]` where <n>
   is the issue number (`feat/12-x`, `fix/12-x`, `chore/12`, ...), regardless of whether GitHub shows
   a formal issue link. Use that to decide "does this item already have a worktree/open PR?" — match
   on the number segment, not on the `feat/` prefix, or you will double-dispatch work that already
   has a `fix/` branch open.
   FIRST reconcile every item with Status `In progress`: confirm its executor is actually alive. The
   reliable signal is the WORKTREE, not the issue number — both SMALL (claude) and LARGE (ralph.sh)
   executors run with their cwd inside the item's worktree, but ralph.sh's argv is just `/bin/bash
   ralph.sh` with no issue ref, so a `pgrep` for the issue number gives false "dead" readings. An
   executor is LIVE if a running process has its cwd at/under the item's worktree — check with
   readlink (NOT `ls`, which on this host is eza and dereferences the symlink):
   `{{ for f in /proc/[0-9]*/cwd; do readlink "$f"; done; }} 2>/dev/null | grep -qF "<worktree-path>"`
   — OR the item has an open PR.
   If NEITHER, the executor died (crash, quota, host restart): post a
   one-line "⚠️ executor gone — re-dispatching" note, `git worktree remove --force` any stale worktree,
   and set the item back to `Ready For Agent` so it re-enters dispatch below. NEVER leave a dead item
   `In progress` — it wedges a concurrency slot forever (this is the #1 failure mode).
   THEN dispatch: for each item with Status `Ready For Agent` that ALSO has a human (non-AI-generated)
   comment saying "approved". Each feature is implemented in its OWN git worktree, in the item's repo,
   so several can run concurrently. Count only items with a LIVE `In progress` executor toward the cap;
   if that is already {CONCURRENCY} or more, dispatch nothing this pass (slots full).
   For each approvable item, up to the {CONCURRENCY} cap:
     - Skip it if it already has a LIVE executor (per the worktree-cwd check above) or an open PR.
     - In the item's repo clone, create an isolated worktree under that repo's worktrees base:
       `git worktree add <worktrees base>/<slug> -b feat/<n>-<slug>` (branch off origin/master).
       Set Status to `In progress`.
     - Use the item's Size field to route: XS/S/M -> SMALL path, L/XL -> LARGE path (if Size is
       empty, judge from the brief).
     - a. SMALL feature -> launch a SEPARATE, DETACHED claude process (not an in-process agent, so
          telemetry is tagged correctly AND it outlives this pass) in that worktree, ALWAYS via the
          wrapper so the OTEL usage_mode=ralph tag can't leak (never call bare `claude` for loop work).
          Detach with setsid + nohup and log to /tmp/impl-<n>.log so a later pass can reconcile it
          (liveness is detected by the executor's cwd = worktree, per step 3):
          `cd <worktree> && setsid nohup {RALPH_CLAUDE} implementer --permission-mode auto --model sonnet -p "Implement GitHub issue <owner/repo>#<n> in this worktree (branch feat/<n>-<slug>) using /tdd, then open a PR. Ignore anything labeled {IGNORE}." > /tmp/impl-<n>.log 2>&1 &`
          Done when CI is green.
     - b. LARGE feature -> spawn an Opus sub-agent (auto mode, prompt includes the ignore-`{IGNORE}`
          sentence) in that worktree to write a feature-scoped prd.json + progress files, then run
          {RALPH_SH} there (its lock is per-worktree, so instances do not collide).
          Monitor, keep status updated, open a PR, confirm CI green.
   Every background executor must, on completion, `touch "{WAKE}"` and set the item's Status to
   `In review` once its PR is open and CI green (or `Ready For Human` if it failed). When a feature's
   PR is merged, set Status `Done` and remove its worktree with `git worktree remove`.
   One worktree/branch/PR per feature — never bundle features.

4. Unblock `In review` PRs. A PR in `In review` is NOT finished, and nothing else in this loop
   watches it. For each item with Status `In review`, inspect its PR:
   `gh pr view <pr> -R <item repo> --json reviews,comments,commits,mergeable,mergeStateStatus`.
   TWO things strand a PR — check both:
     a. UNADDRESSED FEEDBACK — the newest HUMAN (non-AI, not the executor) review or comment is NEWER
        than the newest commit's committedDate, so the reviewer spoke after the last push. (If the
        last push is newer, the executor already responded — leave it.)
     b. STALE BRANCH — `mergeable` is `CONFLICTING` (master moved under it, usually because a sibling
        PR merged), or `mergeStateStatus` is `BEHIND` (branch protection wants it current). Nothing
        comments when this happens, so it never surfaces as (a) — with several PRs open off one
        master, merging any one of them can strand the rest. `mergeable` may come back `UNKNOWN`
        while GitHub recomputes: treat that as "no verdict", leave the item alone, the next pass
        re-checks it.
   For each item matching (a) or (b), up to the {CONCURRENCY} cap:
     - Skip if it already has a LIVE executor (the worktree-cwd check from step 3) — one is on it.
     - Set Status `In progress` (so it counts toward the cap and reconcile tracks it) and relaunch a
       DETACHED executor in the item's EXISTING worktree — reuse the same worktree/branch/PR, NEVER
       open a second PR:
       For (a), unaddressed feedback:
       `cd <worktree> && setsid nohup {RALPH_CLAUDE} implementer --permission-mode auto --model sonnet -p "Address the review feedback on PR #<pr> for issue <owner/repo>#<n> in this worktree (branch <type>/<n>-<slug>). Read it with 'gh pr view <pr> -R <owner/repo> --json reviews,comments'; if the goal/approach is being questioned, reconcile the change to ONE coherent approach (do not leave half-server/half-client changes); make the fixes with /tdd, commit and push to the SAME branch, then reply to the review summarising what changed. Ignore anything labeled {IGNORE}." > /tmp/review-<pr>.log 2>&1 &`
       For (b), stale branch — rebase only, NO feature work:
       `cd <worktree> && setsid nohup {RALPH_CLAUDE} implementer --permission-mode auto --model sonnet -p "PR #<pr> for issue <owner/repo>#<n> no longer merges into master. In this worktree (branch <type>/<n>-<slug>): 'git fetch origin', rebase onto origin/master, and resolve every conflict KEEPING BOTH SIDES' intent — master's incoming change and this branch's feature. Change nothing else: no new features, no refactors, no drive-by fixes. Then run the test suite (see CLAUDE.md); if it fails, fix only what the rebase broke. Force-push to the SAME branch with --force-with-lease, then comment on the PR listing which files conflicted and how you resolved them. If a conflict is a genuine product decision rather than a mechanical merge, do NOT guess — comment on the PR explaining the choice needed and stop. Ignore anything labeled {IGNORE}." > /tmp/rebase-<pr>.log 2>&1 &`
       On completion it `touch "{WAKE}"` and sets Status back to `In review` (or `Ready For Human`
       if it stopped on a product decision).

All sub-agents run in auto permission mode. Keep diffs minimal. Do not touch `{IGNORE}` items.{autoblock}
"""


def env():
    """Session env + OTEL exporters (from ~/.zshrc) with usage_mode=ralph."""
    e = dict(os.environ)
    e["CLAUDE_CODE_ENABLE_TELEMETRY"]   = "1"
    e["OTEL_METRICS_EXPORTER"]          = "otlp"
    e["OTEL_EXPORTER_OTLP_PROTOCOL"]    = "grpc"
    e["OTEL_EXPORTER_OTLP_ENDPOINT"]    = OTEL_ENDPOINT
    e["OTEL_RESOURCE_ATTRIBUTES"]       = "usage_mode=ralph,agent_type=PM"
    e["RALPH_WAKE"]                     = str(WAKE)
    return e


# ── triggers / state ────────────────────────────────────────────────────────────
def gh_fingerprint(projects, run=None):
    """{repo#number: updatedAt} for open issues+PRs across every repo (ignore label excluded), or None
    if ANY repo could not be read.

    ponytail: REST (`/issues` lists issues AND PRs) so the 60s poll draws on the core quota, not the
    GraphQL one the passes and executors need. None on failure is load-bearing: an empty/partial
    fingerprint used to read as "the board changed", which fired a pass, which burned more quota,
    which failed again — a self-sustaining rate-limit storm."""
    run = run or subprocess.run
    fp = {}
    for p in projects:
        for repo in p["repos"]:
            out = run(["gh", "api", "--paginate", f"repos/{repo}/issues?state=open&per_page=100",
                       "--jq", ".[] | [.number, .updated_at, ([.labels[].name] | join(\",\"))] | @tsv"],
                      capture_output=True, text=True)
            if out.returncode:
                log(f"!! fingerprint {repo}: {(out.stderr or '').strip()[:120] or 'gh failed'}")
                return None
            for line in (out.stdout or "").splitlines():
                num, updated, labels = (line.split("\t") + ["", ""])[:3]
                if IGNORE in labels.split(","):
                    continue
                fp[f"{repo}#{num}"] = updated
    return fp


def acquire_lock():
    """Refuse to start if another orchestrator is alive; reclaim a stale lock."""
    if LOCK.exists():
        pid = int(LOCK.read_text().strip() or 0)
        try:
            os.kill(pid, 0)                       # 0 = liveness probe, no signal sent
            sys.exit(f"orchestrator already running (PID {pid}) — {LOCK}")
        except (ProcessLookupError, ValueError):
            print(f"reclaiming stale lock (PID {pid} dead)")
        except PermissionError:
            sys.exit(f"orchestrator already running (PID {pid}, not ours) — {LOCK}")
    LOCK.write_text(str(os.getpid()))
    atexit.register(lambda: LOCK.unlink(missing_ok=True))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # fire atexit on `kill`


def _json(args):
    """gh -> json. Log stderr on failure: every gh error (auth, scope, rate limit) otherwise
    arrives as an empty list and gets reported as a meaningless '0 items returned'."""
    r = subprocess.run(args, capture_output=True, text=True)
    if r.returncode or not r.stdout.strip():
        log(f"!! {' '.join(args[:3])}: {r.stderr.strip()[:200] or 'empty output'}")
    return json.loads(r.stdout or "[]")


def _ci(rollup):
    if not rollup:
        return "—"
    s = [c.get("conclusion") or c.get("state") or "" for c in rollup]
    if any(x in ("FAILURE", "ERROR", "CANCELLED", "TIMED_OUT") for x in s):
        return "fail"
    if any(x in ("", "PENDING", "IN_PROGRESS", "QUEUED") for x in s):
        return "pending"
    return "pass"


def _repo_ctx(repo, path):
    """(open PRs, branch->worktree-path map) for one repo, for the status join."""
    prs = _json(["gh", "pr", "list", "-R", repo, "--state", "open", "--limit", "200",
                 "--json", "number,headRefName,statusCheckRollup,mergeable,mergeStateStatus"])
    wt, wpath = {}, None
    for line in subprocess.run(["git", "-C", path, "worktree", "list", "--porcelain"],
                               capture_output=True, text=True).stdout.splitlines():
        if line.startswith("worktree "):
            wpath = line[9:]
        elif line.startswith("branch "):
            wt[line[7:].replace("refs/heads/", "")] = wpath
    return prs, wt


def _for_issue(branch, n):
    """True if `branch` belongs to issue n: any <type>/<n>[-slug] (feat/, fix/, chore/, ...)."""
    return re.match(rf"[^/]+/{n}(?:[-/]|$)", branch or "") is not None


# ── board scan ─────────────────────────────────────────────────────────────────
@dataclass
class BoardItem:
    """One project item, joined to its worktree/PR/CI, plus which action it needs (if any)."""
    repo: str                 # "owner/name"
    number: int
    item_id: str              # project item id (for `gh project item-edit --id`)
    title: str
    status: str               # project Status name; "" when unset
    size: str                 # "" when unset
    priority: str             # "" when unset
    labels: list[str]
    worktree: str | None      # path, if a <type>/<n> worktree exists
    pr: dict | None           # {number, headRefName, mergeable, mergeStateStatus, ci}
    reason: str | None        # which actionable rule matched; None = nothing to do


# markers an agent leaves on its own comments — see _is_agent()
AGENT_MARKS = ("🤖", "Product Brief", "Technical Plan", "executor gone", "Generated with [Claude Code]")
PUSH_GRACE = 120   # s: an executor comments on its OWN push within ~30s — that is not review feedback


def _proc_cwds():
    """cwd of every running process. `readlink` over /proc (NOT `ls`, which is eza here and
    dereferences the symlink; NOT pgrep, since ralph.sh's argv carries no issue ref)."""
    out = set()
    for f in Path("/proc").glob("[0-9]*/cwd"):
        try:
            out.add(os.readlink(f))
        except OSError:                       # process exited mid-scan, or not ours to read
            pass
    return out


def _is_live(worktree, cwds):
    """True if some process is sitting at/under `worktree` — i.e. an executor is on this item."""
    return bool(worktree) and any(c == worktree or c.startswith(worktree.rstrip("/") + "/") for c in cwds)


_ME = None


def _me():
    """Login the orchestrator and its executors post as (cached). "" if gh cannot say — then nothing
    counts as agent-authored, which errs toward running the pass."""
    global _ME
    if _ME is None:
        d = _json(["gh", "api", "user"])
        _ME = d.get("login", "") if isinstance(d, dict) else ""
    return _ME


def _is_agent(c, bot):
    """Was this comment/review written by the loop rather than a human?

    ponytail: the executors post under the operator's OWN account here, so the login alone cannot
    tell agent from human — we additionally require an agent marker in the body, which mistakes an
    agent comment for a human one (extra pass) rather than the reverse (silent stall). The two
    identity-free signals (PUSH_GRACE, status_since) carry the cases markers miss. Upgrade path:
    give the executors a dedicated bot account/app token and drop all three heuristics."""
    login = ((c.get("author") or {}).get("login") or "")
    return login.endswith("[bot]") or (bool(bot) and login == bot
                                       and any(m in (c.get("body") or "") for m in AGENT_MARKS))


def _newest(events, *keys):
    """Newest timestamp among `events`, "" if none — ISO-8601 UTC sorts lexicographically."""
    return max([t for e in events for k in keys if (t := str(e.get(k) or ""))], default="")


def _after(t, ref, grace=0):
    """True if timestamp `t` is more than `grace` seconds later than `ref` ("" = no such event)."""
    if not t:
        return False
    if not ref:
        return True
    fix = lambda x: datetime.fromisoformat(x.replace("Z", "+00:00"))
    return (fix(t) - fix(ref)).total_seconds() > grace


def classify(bi, comments=(), reviews=(), commits=(), live=False, bot="", since="", auto=False):
    """Which actionable rule `bi` matches, or None. Pure: all board state arrives as arguments.

    `since` is when the item's Status was last set (see status_since). `auto` = the item's repo opted
    in to auto_approve; only then does AUTO_LABEL stand in for a human approval / license a merge."""
    auto = auto and AUTO_LABEL in bi.labels
    human = [c for c in comments if not _is_agent(c, bot)]
    if bi.status in ("Backlog", "Needs Triage") and PRODUCT_LABEL not in bi.labels:
        return "product"
    if bi.status == "Needs Info":               # ball is ours again once the reporter has replied
        ours = max(_newest([c for c in comments if _is_agent(c, bot)], "createdAt"), since)
        if _after(_newest(human, "createdAt"), ours):
            return "technical"
    if bi.status == "Ready For Agent":
        if auto or any("approved" in (c.get("body") or "").lower() for c in human):
            return "dispatch"
    if bi.status == "In progress" and not live and not bi.pr:
        return "reconcile"                      # dead executor wedging a slot — the #1 failure mode
    if bi.status == "In review":
        spoke = max(_newest(human, "createdAt"),
                    _newest([r for r in reviews if not _is_agent(r, bot)], "submittedAt", "createdAt"))
        if _after(spoke, _newest(commits, "committedDate"), PUSH_GRACE):
            return "review-feedback"            # a reviewer spoke after the last push
        pr = bi.pr or {}                        # UNKNOWN = GitHub still recomputing, not a verdict
        if pr.get("mergeable") == "CONFLICTING" or pr.get("mergeStateStatus") == "BEHIND":
            return "stale-branch"
        if auto and not live and pr.get("ci") == "pass" and pr.get("mergeable") == "MERGEABLE":
            return "merge"                      # green + mergeable but nobody is merging it
    return None


def status_since(p):
    """{project item id: when its Status was last set}, {} if the query fails.

    ponytail: `Needs Info` means the ball has been in the REPORTER's court since that moment, which
    is the only identity-free way to tell "they replied" from "we asked and nobody answered" on a
    board where the loop comments under the operator's own login. One graphql call per project, first
    100 items; items past that fall back to the author check alone. Upgrade path: paginate, or drop
    this once the executors post under their own bot account."""
    q = ('query($id:ID!){node(id:$id){... on ProjectV2{items(first:100){nodes{id '
         'fieldValueByName(name:"Status"){... on ProjectV2ItemFieldSingleSelectValue{updatedAt}}}}}}}')
    d = _json(["gh", "api", "graphql", "-f", f"query={q}", "-f", f"id={p['project_id']}"])
    nodes = [] if not isinstance(d, dict) else \
        (((d.get("data") or {}).get("node") or {}).get("items") or {}).get("nodes") or []
    return {n["id"]: (n.get("fieldValueByName") or {}).get("updatedAt") or "" for n in nodes}


def _issue_comments(repo, n):
    d = _json(["gh", "issue", "view", str(n), "-R", repo, "--json", "comments"])
    return d.get("comments") or [] if isinstance(d, dict) else []


def _pr_activity(repo, pr):
    d = _json(["gh", "pr", "view", str(pr), "-R", repo, "--json", "comments,reviews,commits"])
    if not isinstance(d, dict):
        return [], [], []
    return d.get("comments") or [], d.get("reviews") or [], d.get("commits") or []


def scan_board(p):
    """One pass of board reconnaissance for project `p`: items joined to worktree/PR/CI, classified.

    The single source of board truth — write_status() renders it and run_iteration() gates on it.
    Comments cost a gh call per item, so only the statuses whose rules need them pay for them."""
    ctx = {repo: _repo_ctx(repo, spec["path"]) for repo, spec in p["repos"].items()}
    cwds, bot, since, items = _proc_cwds(), _me(), status_since(p), []
    for it in sorted(project_items(p), key=lambda x: x["content"]["number"]):
        repo, n = it["content"].get("repository") or "", it["content"]["number"]
        prs, wt = ctx.get(repo, ([], {}))
        pr = next((q for q in prs if _for_issue(q["headRefName"], n)), None)
        bi = BoardItem(
            repo=repo, number=n, item_id=it.get("id") or "", title=it.get("title") or "",
            status=it.get("status") or "", size=it.get("size") or "", priority=it.get("priority") or "",
            labels=list(it.get("labels") or []),
            worktree=next((q for b, q in wt.items() if _for_issue(b, n)), None),
            pr=None if pr is None else {
                "number": pr["number"], "headRefName": pr["headRefName"],
                "mergeable": pr.get("mergeable") or "", "mergeStateStatus": pr.get("mergeStateStatus") or "",
                "ci": _ci(pr.get("statusCheckRollup"))},
            reason=None)
        comments, reviews, commits = (), (), ()
        if bi.status in ("Needs Info", "Ready For Agent"):
            comments = _issue_comments(repo, n)
        elif bi.status == "In review" and bi.pr:
            comments, reviews, commits = _pr_activity(repo, bi.pr["number"])
        bi.reason = classify(bi, comments, reviews, commits, live=_is_live(bi.worktree, cwds),
                             bot=bot, since=since.get(bi.item_id, ""),
                             auto=p["repos"].get(repo, {}).get("auto_approve", False))
        items.append(bi)
    return items


def actionable(items):
    """The items that need this pass to do something."""
    return [b for b in items if b.reason]


def write_status(projects, scans=None):
    """Render the board table from scan_board(). Returns {project_number: items} so a caller can
    reuse the scan (one scan per project per wake) instead of paying for a second one."""
    scans = dict(scans or {})
    rows = ["| repo | issue | status | size/prio | worktree | PR | CI | why |",
            "|---|---|---|---|---|---|---|---|"]
    for p in projects:
        num = p["project_number"]
        if scans.get(num) is None:
            try:
                scans[num] = scan_board(p)
            except Exception as ex:      # transient gh failure — render the rest of the board
                log(f"!! status: board scan failed for project #{num}: {ex}")
                scans.pop(num, None)
                continue
        for b in scans[num]:
            if b.status == "Done":
                continue
            rows.append(f"| {b.repo.split('/')[-1] if b.repo else '—'} | #{b.number} {b.title[:28]} | "
                        f"{b.status or '—'} | "
                        f"{b.size or '—'}/{b.priority or '—'} | "
                        f"{Path(b.worktree).name if b.worktree else '—'} | "
                        f"{'#'+str(b.pr['number']) if b.pr else '—'} | "
                        f"{b.pr['ci'] if b.pr else '—'} | "
                        f"{b.reason or '—'} |")
    STATUS_MD.write_text("\n".join(rows) + "\n")
    log(f"status -> {STATUS_MD}")
    return scans


def run_iteration(projects, scans=None):
    """One opus pass per project — but only for projects whose board actually has something to do.

    Returns {project_number: items} for the projects we did NOT run, whose scan is therefore still
    current and can be handed to write_status()."""
    scans, fresh = scans or {}, {}
    for p in projects:
        num = p["project_number"]
        items = scans.get(num)
        try:
            instruction = build_instruction(p, status_opts(p))
        except Exception as ex:  # transient gh failure — skip this project, try again next trigger
            log(f"!! skipping project #{num} this pass: {ex}")
            if items is not None:
                fresh[num] = items
            continue
        if items is None:
            try:
                items = scan_board(p)
            except Exception as ex:
                # ponytail: the gate fails OPEN. One wasted opus pass is far cheaper than a loop that
                # silently no-ops because gh hiccuped. Upgrade path: retry the scan once before giving up.
                log(f"!! board scan failed for project #{num} ({ex}) — running the pass anyway")
                items = None
        if items is not None:
            todo = actionable(items)
            if not todo:
                log(f"idle — nothing actionable for project #{num}")
                fresh[num] = items
                continue
            log(f"board: {len(todo)} actionable — {', '.join(sorted({b.reason for b in todo}))}")
        log(f"=== iteration: claude (opus) for project #{num} ({', '.join(p['repos'])}) ===")
        out = _run_pass([RALPH_CLAUDE, "PM", "-p", instruction, "--model", MODEL,
                         "--permission-mode", "auto"])
        global QUOTA_UNTIL
        until = quota_reset(out or "")
        if until:
            QUOTA_UNTIL = max(QUOTA_UNTIL, until)
            log(f"quota hit — will wake at {datetime.fromtimestamp(until).strftime('%H:%M')}")
    return fresh


def _run_pass(cmd):
    """Run one opus pass, streaming its output to our stdout; returns the output for quota parsing."""
    proc = subprocess.Popen(cmd, env=env(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = []
    for line in proc.stdout:
        print(line, end="", flush=True)
        out.append(line)
    proc.wait()
    return "".join(out)


QUOTA_UNTIL = 0.0   # epoch: when the subscription quota resets (0 = not limited)


def quota_reset(text, now=None):
    """Epoch to wake at (reset + 60s) if `text` carries a Claude 'hit your limit … resets 7:20am (TZ)'
    message, else None. Unparseable reset -> None (the 1h timer still covers it)."""
    if not re.search(r"hit your (?:\S+ )?limit", text):
        return None
    m = re.search(r"resets\s+(\d{1,2})(?::(\d{2}))?\s*([ap]m)\s*\(([^)]+)\)", text, re.I)
    if not m:
        return None
    from zoneinfo import ZoneInfo
    from datetime import timedelta
    try:
        tz = ZoneInfo(m.group(4))
    except Exception:
        return None
    h = int(m.group(1)) % 12 + (12 if m.group(3).lower() == "pm" else 0)
    cur = datetime.fromtimestamp(now if now is not None else time.time(), tz)
    at = cur.replace(hour=h, minute=int(m.group(2) or 0), second=0, microsecond=0)
    if at <= cur:
        at += timedelta(days=1)
    return at.timestamp() + 60


def wait_for_trigger(projects, seen, wake_mtime):
    """Block until a GitHub change, a wake-file touch, or MAX_WAIT. Returns reason."""
    deadline = time.time() + MAX_WAIT
    if QUOTA_UNTIL > time.time():                  # quota-limited: wake right after the reset instead
        deadline = min(deadline, QUOTA_UNTIL)
    while time.time() < deadline:
        time.sleep(max(0, min(POLL, deadline - time.time())))
        if WAKE.exists() and WAKE.stat().st_mtime != wake_mtime:
            return "process-done"
        fp = gh_fingerprint(projects)
        if fp is not None and seen is not None and fp != seen:
            return "github-update"
        if seen is None and fp is not None:      # baseline failed earlier: adopt the first good read
            seen = fp
    return "quota-reset" if 0 < QUOTA_UNTIL <= time.time() + 1 else "1h-timer"


def main(projects):
    STATE.mkdir(parents=True, exist_ok=True)
    acquire_lock()
    WAKE.touch()
    log(f"orchestrating {len(projects)} project(s): {', '.join(r for p in projects for r in p['repos'])}")
    scans = {}
    while True:
        write_status(projects, run_iteration(projects, scans))   # re-scans only what the pass changed
        seen, wake_mtime = gh_fingerprint(projects), WAKE.stat().st_mtime  # re-baseline after our run
        reason = wait_for_trigger(projects, seen, wake_mtime)
        scans = write_status(projects)                           # refresh on every wake; feeds the gate
        log(f"=== trigger: {reason} ===")


def selftest():
    # fingerprint: REST rows parse, ignore label filtered, ANY failure -> None (never a fake change)
    class _P:
        def __init__(self, out="", rc=0): self.stdout, self.returncode, self.stderr = out, rc, "boom"
    pj = [normalize({"repo": "x/y", "project_number": 1, "project_id": "P", "status_field_id": "F",
                     "path": "/tmp/x"})]
    rows = f"3\t2026-01-01T00:00:00Z\tbug\n4\t2026-01-02T00:00:00Z\t{IGNORE},x\n"
    assert gh_fingerprint(pj, run=lambda *a, **k: _P(rows)) == {"x/y#3": "2026-01-01T00:00:00Z"}
    assert gh_fingerprint(pj, run=lambda *a, **k: _P("", 1)) is None
    # quota parsing: wake 60s after the stated reset, in the stated tz; next day if already past
    from zoneinfo import ZoneInfo
    jt = ZoneInfo("Asia/Jerusalem")
    n = datetime(2026, 11, 27, 4, 5, tzinfo=jt).timestamp()
    msg = "You've hit your 5-hour limit · resets 7:20am (Asia/Jerusalem)"
    assert quota_reset(msg, n) == datetime(2026, 11, 27, 7, 20, tzinfo=jt).timestamp() + 60
    late = datetime(2026, 11, 27, 8, 0, tzinfo=jt).timestamp()
    assert quota_reset(msg, late) == datetime(2026, 11, 28, 7, 20, tzinfo=jt).timestamp() + 60
    assert quota_reset("You've hit your limit · resets 3pm (UTC)", n) is not None
    assert quota_reset("all fine", n) is None and quota_reset("hit your limit, no time", n) is None
    e = env()
    assert e["OTEL_RESOURCE_ATTRIBUTES"] == "usage_mode=ralph,agent_type=PM"
    assert e["OTEL_EXPORTER_OTLP_ENDPOINT"] == OTEL_ENDPOINT
    assert e["RALPH_WAKE"].endswith("wake")
    assert _ci([]) == "—" and _ci([{"conclusion": "SUCCESS"}]) == "pass"
    assert _ci([{"conclusion": "SUCCESS"}, {"status": "IN_PROGRESS"}]) == "pending"
    assert _ci([{"conclusion": "FAILURE"}]) == "fail"
    a = {"issue1": "t0", "pr2": "t0"}
    assert a == dict(a) and a != {**a, "issue1": "t1"}  # change detection is dict-inequality
    # single-repo shorthand: derives owner + default worktrees, requires core ids
    q = normalize({"repo": "x/y", "project_number": 1, "project_id": "P",
                   "status_field_id": "F", "path": "/tmp/x"})
    assert list(q["repos"]) == ["x/y"] and q["owner"] == "x"
    assert q["repos"]["x/y"]["worktrees"] == "/tmp/x-wt"
    h = normalize({"repo": "x/y", "project_number": 1, "project_id": "P", "status_field_id": "F",
                   "path": "~/dev/x", "worktrees": "~/dev/x-wt"})["repos"]["x/y"]
    assert h["path"] == os.path.expanduser("~/dev/x") and "~" not in h["worktrees"]  # `git -C` needs it
    try:
        normalize({"repo": "x/y"}); assert False, "should reject missing ids"
    except SystemExit:
        pass
    # multi-repo board: a project spanning two repos (bare path + {path,worktrees})
    p = normalize({"project_number": 2, "project_id": "PID", "status_field_id": "FID",
                   "notes": "RTL Hebrew mockups.",
                   "repos": {"acme/widget": "/tmp/widget", "acme/api": {"path": "/tmp/api"}}})
    assert p["owner"] == "acme"
    assert p["repos"]["acme/widget"]["worktrees"] == "/tmp/widget-wt"
    assert p["repos"]["acme/api"]["path"] == "/tmp/api" and p["repos"]["acme/api"]["owner"] == "acme"
    # instruction carries every repo + ids + notes, flags multi-repo routing, no hardcoded app name
    s = build_instruction(p, {"Done": "opt9"})
    assert "acme/widget" in s and "acme/api" in s and "/tmp/widget-wt" in s
    assert "spans MULTIPLE repos" in s and "content.repository" in s
    assert "AUTO-APPROVE" not in s and AUTO_LABEL not in s      # no repo opted in -> no auto rules
    pa = normalize({"project_number": 2, "project_id": "PID", "status_field_id": "FID",
                    "repos": {"acme/widget": "/tmp/widget",
                              "acme/api": {"path": "/tmp/api", "auto_approve": True,
                                           "default_branch": "main"}}})
    sa = build_instruction(pa, {"Done": "opt9"})
    assert "AUTO-APPROVE repos (acme/api)" in sa and AUTO_LABEL in sa and "ESCALATION TRIGGERS" in sa
    assert "default branch `main`" in sa and "gh pr merge" in sa and "Blocked by #n" in sa
    assert pa["repos"]["acme/widget"]["auto_approve"] is False
    assert "RTL Hebrew mockups." in s and "Done=opt9" in s and RALPH_SH in s and "CompuDesk" not in s
    # dispatched agents spawn via the wrapper so usage_mode=ralph can't leak to interactive
    assert RALPH_CLAUDE in s and "bare `claude`" in s
    # tool paths derive from this file's dir, so the repo runs from any checkout location
    assert RALPH_SH == str(Path(__file__).resolve().parent / "ralph.sh") and Path(RALPH_SH).exists()
    assert Path(RALPH_CLAUDE).exists()
    # two-phase triage: product gate stamps the label before the technical phase
    assert PRODUCT_LABEL in s and "PRODUCT phase" in s and "TECHNICAL phase" in s
    assert "HTML mockup" in s  # UI features get a visual mockup in the product phase
    # dead-executor reconciliation: In-progress items whose executor vanished must be recovered
    assert "Reconcile" in s and "executor gone" in s and "wedges a concurrency slot" in s
    assert "/proc/[0-9]*/cwd" in s and "readlink" in s  # liveness via readlink on worktree cwd (eza-safe)
    assert "setsid nohup" in s  # SMALL executors detach so they outlive the pass
    # branch join is by number segment, so fix/ and chore/ branches count as the item's work
    assert "<type>/<n>[-slug]" in s and "not on the `feat/` prefix" in s
    assert _for_issue("fix/12-x", 12) and _for_issue("chore/12", 12) and _for_issue("feat/12/a", 12)
    assert not _for_issue("feat/120-x", 12) and not _for_issue("feat/x-12", 12) and not _for_issue(None, 12)
    # review-feedback: In-review PRs with unaddressed human comments get re-dispatched (not stranded)
    assert "Address the review feedback" in s and "UNADDRESSED FEEDBACK" in s and "SAME branch" in s
    # stale-branch trigger: a sibling PR merging strands the rest, and nothing comments when it does
    assert "mergeable,mergeStateStatus" in s and "CONFLICTING" in s and "BEHIND" in s
    assert "UNKNOWN" in s                      # transient GitHub state must not trigger a rebase
    assert "rebase onto origin/master" in s and "--force-with-lease" in s
    assert "no new features" in s              # rebase executor must not smuggle in feature work
    # ── board scan / actionable gate ──────────────────────────────────────────
    def bi(**kw):
        d = dict(repo="acme/widget", number=7, item_id="PVTI_x", title="t", status="", size="",
                 priority="", labels=[], worktree=None, pr=None, reason=None)
        return BoardItem(**{**d, **kw})

    def hum(t, body="", who="reporter"):
        return {"author": {"login": who}, "body": body, "createdAt": t, "submittedAt": t}

    def bot(t, body="Product Brief\n\nProblem: ..."):
        return {"author": {"login": "ralphbot"}, "body": body, "createdAt": t, "submittedAt": t}

    B = "ralphbot"
    T1, T2, T3 = "2026-09-16T10:00:00Z", "2026-09-16T11:00:00Z", "2026-09-16T12:00:00Z"
    # product: untriaged and not yet product-approved
    assert classify(bi(status="Backlog"), bot=B) == "product"
    assert classify(bi(status="Needs Triage"), bot=B) == "product"
    assert classify(bi(status="Backlog", labels=[PRODUCT_LABEL]), bot=B) is None
    # technical: the reporter answered the brief (newest comment is human and newer than ours)
    assert classify(bi(status="Needs Info"), [bot(T1), hum(T2)], bot=B) == "technical"
    assert classify(bi(status="Needs Info"), [hum(T1), bot(T2)], bot=B) is None
    assert classify(bi(status="Needs Info"), [], bot=B) is None
    # ...but only if they replied AFTER we handed them the ball (status_since), not months before
    assert classify(bi(status="Needs Info"), [hum(T1)], bot=B, since=T2) is None
    assert classify(bi(status="Needs Info"), [hum(T3)], bot=B, since=T2) == "technical"
    # a reply under OUR login but with no agent marker counts as human — the gate errs open
    assert classify(bi(status="Needs Info"), [bot(T1), hum(T2, who=B)], bot=B) == "technical"
    assert classify(bi(status="Needs Info"), [hum(T1), bot(T2, "🤖 pushed a fix")], bot=B) is None
    # dispatch: a human approval on a Ready For Agent item (the bot approving itself is not one)
    assert classify(bi(status="Ready For Agent"), [hum(T1, "Approved, go ahead")], bot=B) == "dispatch"
    assert classify(bi(status="Ready For Agent"), [hum(T1, "looks fine")], bot=B) is None
    assert classify(bi(status="Ready For Agent"), [bot(T1, "Technical Plan — approved")], bot=B) is None
    # reconcile: In progress with neither a live executor nor an open PR = the executor died
    assert classify(bi(status="In progress", worktree="/tmp/wt"), live=False, bot=B) == "reconcile"
    assert classify(bi(status="In progress", worktree="/tmp/wt"), live=True, bot=B) is None
    assert classify(bi(status="In progress", pr={"number": 3, "mergeable": "MERGEABLE"}), bot=B) is None
    ok = {"number": 3, "headRefName": "feat/7-x", "mergeable": "MERGEABLE",
          "mergeStateStatus": "CLEAN", "ci": "pass"}
    # review-feedback: the reviewer spoke after the last push (but not before it)
    assert classify(bi(status="In review", pr=ok), [hum(T3)], commits=[{"committedDate": T2}],
                    bot=B) == "review-feedback"
    assert classify(bi(status="In review", pr=ok), [hum(T1)], commits=[{"committedDate": T2}],
                    bot=B) is None
    assert classify(bi(status="In review", pr=ok), reviews=[hum(T3)],
                    commits=[{"committedDate": T2}], bot=B) == "review-feedback"
    assert classify(bi(status="In review", pr=ok), [bot(T3, "🤖 pushed")],
                    commits=[{"committedDate": T2}], bot=B) is None       # our own comment isn't feedback
    # the executor's "Addressed in <sha>" lands seconds after its own push — not review feedback
    assert classify(bi(status="In review", pr=ok), [hum("2026-09-16T10:00:25Z", "Addressed in 1a1e3b0")],
                    commits=[{"committedDate": T1}], bot=B) is None
    # stale-branch: a sibling merge stranded it — nothing comments, so only merge state shows it
    assert classify(bi(status="In review", pr={**ok, "mergeable": "CONFLICTING"}), bot=B) == "stale-branch"
    assert classify(bi(status="In review", pr={**ok, "mergeStateStatus": "BEHIND"}), bot=B) == "stale-branch"
    assert classify(bi(status="In review", pr={**ok, "mergeable": "UNKNOWN"}), bot=B) is None  # recomputing
    # auto-approve: AUTO_LABEL dispatches and merges ONLY on an opted-in repo
    A = [AUTO_LABEL]
    assert classify(bi(status="Ready For Agent", labels=A), bot=B, auto=True) == "dispatch"
    assert classify(bi(status="Ready For Agent", labels=A), bot=B) is None          # repo not opted in
    assert classify(bi(status="Ready For Agent"), bot=B, auto=True) is None         # opted in, no label
    assert classify(bi(status="In review", labels=A, pr=ok), bot=B, auto=True) == "merge"
    assert classify(bi(status="In review", labels=A, pr=ok), bot=B, auto=True, live=True) is None
    assert classify(bi(status="In review", labels=A, pr={**ok, "ci": "pending"}), bot=B, auto=True) is None
    assert classify(bi(status="In review", labels=A, pr=ok), bot=B) is None
    assert classify(bi(status="In review", pr=ok), bot=B, auto=True) is None
    # review feedback still wins over merging
    assert classify(bi(status="In review", labels=A, pr=ok), [hum(T3)], commits=[{"committedDate": T2}],
                    bot=B, auto=True) == "review-feedback"
    # parked boards are exactly what the gate is for
    assert classify(bi(status="Ready For Human"), bot=B) is None
    assert classify(bi(status="Done"), bot=B) is None
    parked = [bi(status="Ready For Human"), bi(status="Needs Info", number=8)]
    todo = bi(status="Backlog", number=9)
    for b in parked + [todo]:
        b.reason = classify(b, bot=B)
    assert actionable(parked) == [] and [b.number for b in actionable(parked + [todo])] == [9]
    # liveness is cwd-at-or-under the worktree, via readlink over /proc (eza-safe, ralph.sh-safe)
    assert _is_live("/tmp/wt", {"/tmp/wt"}) and _is_live("/tmp/wt", {"/other", "/tmp/wt/src"})
    assert not _is_live("/tmp/wt", {"/tmp/wt-other", "/tmp/w"}) and not _is_live(None, {"/tmp/wt"})
    assert os.getcwd() in _proc_cwds()                    # our own /proc/<pid>/cwd must resolve
    # agent-vs-human: bots always agent; our login only when the body carries an agent marker
    assert _is_agent({"author": {"login": "github-actions[bot]"}, "body": "ci"}, B)
    assert not _is_agent({"author": {"login": B}, "body": "yes please"}, B)
    assert not _is_agent({"author": {"login": "reporter"}, "body": "Product Brief"}, B)
    assert _newest([], "createdAt") == "" and _newest([hum(T1), hum(T3)], "createdAt") == T3
    assert _after(T2, T1) and not _after(T1, T2) and _after(T1, "") and not _after("", T1)
    assert not _after("2026-09-16T10:00:30Z", T1, PUSH_GRACE) and _after(T2, T1, PUSH_GRACE)

    class R:                                          # stand-in for subprocess.CompletedProcess
        def __init__(self, out, rc=0):
            self.stdout, self.returncode, self.stderr = out, rc, ""

    _run = subprocess.run
    try:                                              # status_since parses the graphql shape...
        subprocess.run = lambda *a, **k: R(json.dumps({"data": {"node": {"items": {"nodes": [
            {"id": "I1", "fieldValueByName": {"updatedAt": T1}}, {"id": "I2", "fieldValueByName": None}]}}}}))
        assert status_since({"project_id": "P"}) == {"I1": T1, "I2": ""}
        subprocess.run = lambda *a, **k: R("", 1)     # ...and degrades to the author check alone
        assert status_since({"project_id": "P"}) == {}
    finally:
        subprocess.run = _run
    # the gate itself: idle boards skip the opus pass, scan failures still run it (fail OPEN)
    global scan_board, status_opts, _run_pass, QUOTA_UNTIL
    _scan, _opts, _run, calls = scan_board, status_opts, _run_pass, []
    try:
        status_opts = lambda p: {"Done": "opt9"}
        _run_pass = lambda cmd: calls.append(cmd) or ""
        proj = [normalize({"repo": "x/y", "project_number": 9, "project_id": "P",
                           "status_field_id": "F", "path": "/tmp/x"})]
        scan_board = lambda p: [bi(status="Ready For Human"), bi(status="Done", number=8)]
        kept = run_iteration(proj)
        assert calls == [], "idle board must not spawn claude"
        assert [b.status for b in kept[9]] == ["Ready For Human", "Done"]  # scan handed to write_status
        scan_board = lambda p: [bi(status="Backlog", reason="product")]
        run_iteration(proj)
        assert len(calls) == 1, "actionable board must run the pass"
        def boom(p):
            raise RuntimeError("gh hiccup")
        scan_board = boom
        run_iteration(proj)
        assert len(calls) == 2, "fail OPEN: a broken scan must still run the pass"
        status_opts = boom                        # status_opts failure still skips the project
        run_iteration(proj)
        assert len(calls) == 2
        # a pass that hits the quota arms QUOTA_UNTIL so wait_for_trigger wakes at the reset
        status_opts = lambda p: {"Done": "opt9"}
        _run_pass = lambda cmd: calls.append(cmd) or "You've hit your 5-hour limit · resets 7:20am (UTC)"
        QUOTA_UNTIL = 0.0
        run_iteration(proj)
        assert QUOTA_UNTIL > time.time(), "quota message must arm the reset wake"
    finally:
        scan_board, status_opts, _run_pass, QUOTA_UNTIL = _scan, _opts, _run, 0.0

    # lock acquire -> reject second -> release
    global LOCK
    LOCK = STATE / "selftest.lock"
    STATE.mkdir(parents=True, exist_ok=True)
    acquire_lock()
    assert LOCK.exists() and LOCK.read_text().strip() == str(os.getpid())
    LOCK.write_text("999999")                       # impersonate a dead PID
    try:
        acquire_lock(); print("reclaimed stale lock ok")
    finally:
        LOCK.unlink(missing_ok=True)
    print("selftest ok")


def parse_args(argv):
    ap = argparse.ArgumentParser(description="Multi-repo Ralph triage/dispatch orchestrator loop.")
    ap.add_argument("--config", default=str(CONFIG),
                    help=f"projects config JSON (default {CONFIG})")
    ap.add_argument("--repo", help="single-repo mode owner/name (overrides --config)")
    ap.add_argument("--project", type=int, help="GitHub Project number (single-repo mode)")
    ap.add_argument("--project-id", help="Project node id PVT_... (single-repo mode)")
    ap.add_argument("--status-field", help="Status field id PVTSSF_... (single-repo mode)")
    ap.add_argument("--path", help="local repo path (single-repo mode)")
    ap.add_argument("--worktrees", help="worktrees base dir (default <path>/../<name>-wt)")
    ap.add_argument("--notes", help="project-specific prompt notes (single-repo mode)")
    ap.add_argument("--selftest", action="store_true")
    return ap.parse_args(argv)


if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.selftest:
        selftest()
    else:
        main(load_projects(args))
