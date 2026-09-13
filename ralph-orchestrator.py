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
from pathlib import Path

MODEL    = "opus"
IGNORE   = "ultra-ralph"            # skip issues/PRs with this label, this session
PRODUCT_LABEL = "product-approved"  # stamped when the product phase is agreed with the reporter
RALPH_SH = "/data/dev/skills/ralph.sh"
RALPH_CLAUDE = "/data/dev/skills/ralph-claude.sh"  # wrapper: always tags usage_mode=ralph
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
    spec.setdefault("worktrees", str(Path(spec["path"]).parent / (Path(spec["path"]).name + "-wt")))
    spec["owner"] = repo.split("/")[0]
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
                          for r, spec in p["repos"].items())
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

All sub-agents run in auto permission mode. Keep diffs minimal. Do not touch `{IGNORE}` items.
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
def gh_fingerprint(projects):
    """{repo+kind+number: updatedAt} for open issues+PRs across every repo, excluding the ignore label."""
    fp = {}
    for p in projects:
        for repo in p["repos"]:
            for kind in ("issue", "pr"):
                out = subprocess.run(
                    ["gh", kind, "list", "-R", repo, "--state", "open", "--limit", "200",
                     "--json", "number,updatedAt,labels"],
                    capture_output=True, text=True)
                for it in json.loads(out.stdout or "[]"):
                    if any(l["name"] == IGNORE for l in it.get("labels", [])):
                        continue
                    fp[f"{repo}{kind}{it['number']}"] = it["updatedAt"]
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
    return json.loads(subprocess.run(args, capture_output=True, text=True).stdout or "[]")


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
                 "--json", "number,headRefName,statusCheckRollup"])
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


def write_status(projects):
    """Join project items -> worktree -> PR -> CI on the <type>/<n> branch convention, per item's repo."""
    rows = ["| repo | issue | status | size/prio | worktree | PR | CI |",
            "|---|---|---|---|---|---|---|"]
    for p in projects:
        ctx = {repo: _repo_ctx(repo, spec["path"]) for repo, spec in p["repos"].items()}
        for it in sorted(project_items(p), key=lambda x: x["content"]["number"]):
            if it.get("status") == "Done":
                continue
            repo = it["content"].get("repository")
            prs, wt = ctx.get(repo, ([], {}))
            n = it["content"]["number"]
            wtp = next((q for b, q in wt.items() if _for_issue(b, n)), None)
            pr = next((q for q in prs if _for_issue(q["headRefName"], n)), None)
            rows.append(f"| {repo.split('/')[-1] if repo else '—'} | #{n} {it.get('title','')[:28]} | "
                        f"{it.get('status') or '—'} | "
                        f"{it.get('size') or '—'}/{it.get('priority') or '—'} | "
                        f"{Path(wtp).name if wtp else '—'} | "
                        f"{'#'+str(pr['number']) if pr else '—'} | "
                        f"{_ci(pr['statusCheckRollup']) if pr else '—'} |")
    STATUS_MD.write_text("\n".join(rows) + "\n")
    log(f"status -> {STATUS_MD}")


def run_iteration(projects):
    for p in projects:
        log(f"=== iteration: claude (opus) for project #{p['project_number']} ({', '.join(p['repos'])}) ===")
        try:
            instruction = build_instruction(p, status_opts(p))
        except Exception as ex:  # transient gh failure — skip this project, try again next trigger
            log(f"!! skipping project #{p['project_number']} this pass: {ex}")
            continue
        subprocess.run(
            [RALPH_CLAUDE, "PM", "-p", instruction, "--model", MODEL, "--permission-mode", "auto"],
            env=env())


def wait_for_trigger(projects, seen, wake_mtime):
    """Block until a GitHub change, a wake-file touch, or MAX_WAIT. Returns reason."""
    deadline = time.time() + MAX_WAIT
    while time.time() < deadline:
        time.sleep(POLL)
        if WAKE.exists() and WAKE.stat().st_mtime != wake_mtime:
            return "process-done"
        if gh_fingerprint(projects) != seen:
            return "github-update"
    return "1h-timer"


def main(projects):
    STATE.mkdir(parents=True, exist_ok=True)
    acquire_lock()
    WAKE.touch()
    log(f"orchestrating {len(projects)} project(s): {', '.join(r for p in projects for r in p['repos'])}")
    while True:
        run_iteration(projects)
        write_status(projects)
        seen, wake_mtime = gh_fingerprint(projects), WAKE.stat().st_mtime  # re-baseline after our run
        reason = wait_for_trigger(projects, seen, wake_mtime)
        write_status(projects)                                             # refresh on every wake
        log(f"=== trigger: {reason} ===")


def selftest():
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
    assert "RTL Hebrew mockups." in s and "Done=opt9" in s and RALPH_SH in s and "CompuDesk" not in s
    # dispatched agents spawn via the wrapper so usage_mode=ralph can't leak to interactive
    assert RALPH_CLAUDE in s and "bare `claude`" in s
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
