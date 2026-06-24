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
import argparse, atexit, json, os, signal, subprocess, sys, time
from pathlib import Path

MODEL    = "opus"
IGNORE   = "ultra-ralph"            # skip issues/PRs with this label, this session
RALPH_SH = "/data/dev/skills/ralph.sh"
OTEL_ENDPOINT = "http://192.168.11.155:4317"

POLL        = 60                   # seconds between GitHub polls
MAX_WAIT    = 3600                 # 1 hour hard trigger
CONCURRENCY = 3                    # max features in flight per project (one worktree each)
STATE       = Path.home() / ".ralph-orchestrator"
WAKE        = STATE / "wake"
LOCK        = STATE / "orchestrator.lock"
STATUS_MD   = STATE / "status.md"                  # `watch cat ~/.ralph-orchestrator/status.md`
CONFIG      = STATE / "projects.json"


# ── project config ────────────────────────────────────────────────────────────
def normalize(p):
    """Fill derived/default fields on a project dict."""
    missing = [k for k in ("repo", "project_number", "project_id", "status_field_id", "path") if not p.get(k)]
    if missing:
        sys.exit(f"project config missing {missing}: {p}")
    p.setdefault("notes", "")
    p.setdefault("worktrees", str(Path(p["path"]).parent / (Path(p["path"]).name + "-wt")))
    p["owner"] = p["repo"].split("/")[0]
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
    status = next(f for f in fields if f.get("id") == p["status_field_id"])
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
    repo, owner, num = p["repo"], p["owner"], p["project_number"]
    pid, fid, wts = p["project_id"], p["status_field_id"], p["worktrees"]
    notes = f"\n   - Project-specific notes: {p['notes']}" if p.get("notes") else ""
    return f"""You are the triage+dispatch orchestrator for repo {repo}.
Run ONE full pass, then exit (an outer loop re-invokes you on the next trigger).

Triage state lives in GitHub Project #{num} (owner {owner}) Status field, NOT in labels.
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

1. Triage items whose Status is `Backlog` or `Needs Triage` (use /triage). For each: either move to
   `Ready For Agent` with a clear implementation brief (post the brief as an issue comment), or move
   to `Needs Info` / `Ready For Human` with why.
   - Follow the brief/implementation conventions documented in this repo's CLAUDE.md (mockup style,
     language/RTL, layout). If CLAUDE.md requires a mockup for UI features, the brief MUST include one.
   - If an issue is too large for one PR, split it with /to-issues instead of briefing it.{notes}

2. Dispatch: for each item with Status `Ready For Agent` that ALSO has a human (non-AI-generated)
   comment saying "approved". Each feature is implemented in its OWN git worktree so several can
   run concurrently. Before dispatching, count items with Status `In progress` in this project: if
   that is already {CONCURRENCY} or more, dispatch nothing this pass (the slots are full).
   For each approvable item, up to the {CONCURRENCY} cap:
     - Skip it if its Status is already `In progress` (in flight) or it already has an open PR.
     - Create an isolated worktree: `git worktree add {wts}/<slug> -b feat/<n>-<slug>`
       (branch off origin/master, in repo {p["path"]}). Set Status to `In progress`.
     - Use the item's Size field to route: XS/S/M -> SMALL path, L/XL -> LARGE path (if Size is
       empty, judge from the brief).
     - a. SMALL feature -> launch a SEPARATE backgrounded claude process (not an in-process agent,
          so telemetry is tagged correctly) in that worktree:
          `OTEL_RESOURCE_ATTRIBUTES="usage_mode=ralph,agent_type=implementer" claude --permission-mode auto --model sonnet -p "<implement issue using /tdd, open a PR. Ignore anything labeled {IGNORE}.>"`
          Done when CI is green.
     - b. LARGE feature -> spawn an Opus sub-agent (auto mode, prompt includes the ignore-`{IGNORE}`
          sentence) in that worktree to write a feature-scoped prd.json + progress files, then run
          {RALPH_SH} there (its lock is per-worktree, so instances do not collide).
          Monitor, keep status updated, open a PR, confirm CI green.
   Every background executor must, on completion, `touch "{WAKE}"` and set the item's Status to
   `In review` once its PR is open and CI green (or `Ready For Human` if it failed). When a feature's
   PR is merged, set Status `Done` and remove its worktree with `git worktree remove`.
   One worktree/branch/PR per feature — never bundle features.

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
    """{repo+kind+number: updatedAt} for open issues+PRs across all repos, excluding the ignore label."""
    fp = {}
    for p in projects:
        for kind in ("issue", "pr"):
            out = subprocess.run(
                ["gh", kind, "list", "-R", p["repo"], "--state", "open", "--limit", "200",
                 "--json", "number,updatedAt,labels"],
                capture_output=True, text=True)
            for it in json.loads(out.stdout or "[]"):
                if any(l["name"] == IGNORE for l in it.get("labels", [])):
                    continue
                fp[f"{p['repo']}{kind}{it['number']}"] = it["updatedAt"]
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


def write_status(projects):
    """Join project items -> worktree -> PR -> CI on the feat/<n>- branch prefix, all repos."""
    rows = ["| repo | issue | status | size/prio | worktree | PR | CI |",
            "|---|---|---|---|---|---|---|"]
    for p in projects:
        prs = _json(["gh", "pr", "list", "-R", p["repo"], "--state", "open", "--limit", "200",
                     "--json", "number,headRefName,statusCheckRollup"])
        # branch -> worktree path (worktrees of this project's local repo)
        wt, path = {}, None
        for line in subprocess.run(["git", "-C", p["path"], "worktree", "list", "--porcelain"],
                                   capture_output=True, text=True).stdout.splitlines():
            if line.startswith("worktree "):
                path = line[9:]
            elif line.startswith("branch "):
                wt[line[7:].replace("refs/heads/", "")] = path

        short = p["repo"].split("/")[-1]
        for it in sorted(project_items(p), key=lambda x: x["content"]["number"]):
            if it.get("status") == "Done":
                continue
            n = it["content"]["number"]
            pre = f"feat/{n}-"
            wtp = next((q for b, q in wt.items() if b.startswith(pre)), None)
            pr = next((q for q in prs if q["headRefName"].startswith(pre)), None)
            rows.append(f"| {short} | #{n} {it.get('title','')[:28]} | {it.get('status') or '—'} | "
                        f"{it.get('size') or '—'}/{it.get('priority') or '—'} | "
                        f"{Path(wtp).name if wtp else '—'} | "
                        f"{'#'+str(pr['number']) if pr else '—'} | "
                        f"{_ci(pr['statusCheckRollup']) if pr else '—'} |")
    STATUS_MD.write_text("\n".join(rows) + "\n")
    print(f"status -> {STATUS_MD}", flush=True)


def run_iteration(projects):
    for p in projects:
        print(f"=== iteration: claude (opus) for {p['repo']} ===", flush=True)
        subprocess.run(
            ["claude", "-p", build_instruction(p, status_opts(p)), "--model", MODEL,
             "--permission-mode", "auto"],
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
    print(f"orchestrating {len(projects)} project(s): {', '.join(p['repo'] for p in projects)}", flush=True)
    while True:
        run_iteration(projects)
        write_status(projects)
        seen, wake_mtime = gh_fingerprint(projects), WAKE.stat().st_mtime  # re-baseline after our run
        reason = wait_for_trigger(projects, seen, wake_mtime)
        write_status(projects)                                             # refresh on every wake
        print(f"=== trigger: {reason} ===", flush=True)


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
    # project normalize: derives owner + default worktrees, requires the core ids
    p = normalize({"repo": "acme/widget", "project_number": 2, "project_id": "PID",
                   "status_field_id": "FID", "path": "/tmp/widget", "notes": "RTL Hebrew mockups."})
    assert p["owner"] == "acme" and p["worktrees"] == "/tmp/widget-wt"
    try:
        normalize({"repo": "x/y"}); assert False, "should reject missing ids"
    except SystemExit:
        pass
    # instruction is generic + carries this project's ids, paths, notes — no hardcoded app name
    s = build_instruction(p, {"Done": "opt9"})
    assert "acme/widget" in s and "RTL Hebrew mockups." in s and "/tmp/widget-wt" in s
    assert "Done=opt9" in s and RALPH_SH in s and "CompuDesk" not in s
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
