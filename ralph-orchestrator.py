#!/usr/bin/env python3
"""Outer trigger-loop that runs the triage/dispatch instruction with Claude (opus).

An iteration fires on ANY of three triggers:
  1. a GitHub issue/PR was updated (open, not labeled ultra-ralph)
  2. a spawned sub-agent/process finished  (it touches $RALPH_WAKE on exit)
  3. one hour elapsed since the last run

Each iteration is one blocking `claude -p` opus run in --permission-mode auto.
Long implementation work is launched by that run into the background; when it
finishes it touches the wake file, which wakes this loop to follow up (CI/PR).

ponytail: poll-based, one feature-pass per iteration, single global wake file.
Upgrade path: swap the gh poll for a webhook listener if 60s latency matters.
"""
import atexit, json, os, signal, subprocess, sys, time
from pathlib import Path

REPO        = "roisnir/CompuDesk"
OWNER       = "roisnir"
MODEL       = "opus"
IGNORE      = "ultra-ralph"        # skip issues/PRs with this label, this session

# GitHub Project (v2) "Compugate" — triage state lives in its Status field, not labels.
# Option ids are fetched live (they change on rename), so this never goes stale.
PROJECT_NUM  = 1
PROJECT_ID   = "PVT_kwHOAV4V_c4BYNfT"
STATUS_FIELD = "PVTSSF_lAHOAV4V_c4BYNfTzhTU6K8"
POLL        = 60                   # seconds between GitHub polls
MAX_WAIT    = 3600                 # 1 hour hard trigger
CONCURRENCY = 3                    # max features implemented at once (one worktree each)
WORKTREES   = "/data/dev/compugate/cd-wt"
REPO_DIR    = "/data/dev/compugate/compudesk_v2"   # for `git worktree list`
STATE       = Path.home() / ".ralph-orchestrator"
WAKE        = STATE / "wake"
LOCK        = STATE / "orchestrator.lock"
STATUS_MD   = STATE / "status.md"                  # `watch cat ~/.ralph-orchestrator/status.md`

def status_opts():
    """Live {status name: option id} for the Status field — never hardcode (ids change on rename)."""
    d = _json(["gh", "project", "field-list", str(PROJECT_NUM), "--owner", OWNER, "--format", "json"])
    fields = d.get("fields", []) if isinstance(d, dict) else d
    status = next(f for f in fields if f.get("id") == STATUS_FIELD)
    return {o["name"]: o["id"] for o in status.get("options", [])}


def build_instruction(opts):
    optline = ", ".join(f"{k}={v}" for k, v in opts.items())
    return f"""You are the CompuDesk triage+dispatch orchestrator for repo {REPO}.
Run ONE full pass, then exit (an outer loop re-invokes you on the next trigger).

Triage state lives in the GitHub Project "Compugate" (#{PROJECT_NUM}) Status field, NOT in labels.
To set a status: `gh project item-edit --id <ITEM_ID> --project-id {PROJECT_ID} --field-id {STATUS_FIELD} --single-select-option-id <OPT>`
where <ITEM_ID> is the project item id from `gh project item-list {PROJECT_NUM} --owner {OWNER} --format json`
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
   - UI-feature issues: the brief MUST include an RTL mockup.
   - If an issue is too large for one PR, split it with /to-issues instead of briefing it.

2. Dispatch: for each item with Status `Ready For Agent` that ALSO has a human (non-AI-generated)
   comment saying "approved". Each feature is implemented in its OWN git worktree so several can
   run concurrently. Before dispatching, count items with Status `In progress`: if that is already
   {CONCURRENCY} or more, dispatch nothing this pass (the slots are full).
   For each approvable item, up to the {CONCURRENCY} cap:
     - Skip it if its Status is already `In progress` (in flight) or it already has an open PR.
     - Create an isolated worktree: `git worktree add {WORKTREES}/<slug> -b feat/<n>-<slug>`
       (branch off origin/master). Set Status to `In progress`.
     - Use the item's Size field to route: XS/S/M -> SMALL path, L/XL -> LARGE path (if Size is
       empty, judge from the brief).
     - a. SMALL feature -> launch a SEPARATE backgrounded claude process (not an in-process agent,
          so telemetry is tagged correctly) in that worktree:
          `OTEL_RESOURCE_ATTRIBUTES="usage_mode=ralph,agent_type=implementer" claude --permission-mode auto --model sonnet -p "<implement issue using /tdd, open a PR. Ignore anything labeled {IGNORE}.>"`
          Done when CI is green.
     - b. LARGE feature -> spawn an Opus sub-agent (auto mode, prompt includes the ignore-`{IGNORE}`
          sentence) in that worktree to write a feature-scoped prd.json + progress files, then run
          /data/dev/skills/ralph.sh there (its lock is per-worktree, so instances do not collide).
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
    e["OTEL_EXPORTER_OTLP_ENDPOINT"]    = "http://192.168.11.155:4317"
    e["OTEL_RESOURCE_ATTRIBUTES"]       = "usage_mode=ralph,agent_type=PM"
    e["RALPH_WAKE"]                     = str(WAKE)
    return e


def gh_fingerprint():
    """{number: updatedAt} for open issues+PRs, excluding the ignore label."""
    fp = {}
    for kind in ("issue", "pr"):
        out = subprocess.run(
            ["gh", kind, "list", "-R", REPO, "--state", "open", "--limit", "200",
             "--json", "number,updatedAt,labels"],
            capture_output=True, text=True)
        for it in json.loads(out.stdout or "[]"):
            if any(l["name"] == IGNORE for l in it.get("labels", [])):
                continue
            fp[f"{kind}{it['number']}"] = it["updatedAt"]
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


def project_items():
    """Issue items on the project, excluding the ignore-labeled ones."""
    d = _json(["gh", "project", "item-list", str(PROJECT_NUM), "--owner", OWNER,
               "--format", "json", "--limit", "200"])
    items = d.get("items", []) if isinstance(d, dict) else d
    return [it for it in items
            if it.get("content", {}).get("type") == "Issue"
            and IGNORE not in (it.get("labels") or [])]


def write_status():
    """Join project items -> worktree -> PR -> CI on the feat/<n>- branch prefix."""
    prs = _json(["gh", "pr", "list", "-R", REPO, "--state", "open", "--limit", "200",
                 "--json", "number,headRefName,statusCheckRollup"])
    # branch -> worktree path
    wt, path = {}, None
    for line in subprocess.run(["git", "-C", REPO_DIR, "worktree", "list", "--porcelain"],
                               capture_output=True, text=True).stdout.splitlines():
        if line.startswith("worktree "):
            path = line[9:]
        elif line.startswith("branch "):
            wt[line[7:].replace("refs/heads/", "")] = path

    rows = ["| issue | status | size/prio | worktree | PR | CI |", "|---|---|---|---|---|---|"]
    for it in sorted(project_items(), key=lambda x: x["content"]["number"]):
        if it.get("status") == "Done":
            continue
        n = it["content"]["number"]
        pre = f"feat/{n}-"
        wtp = next((p for b, p in wt.items() if b.startswith(pre)), None)
        pr = next((p for p in prs if p["headRefName"].startswith(pre)), None)
        rows.append(f"| #{n} {it.get('title','')[:30]} | {it.get('status') or '—'} | "
                    f"{it.get('size') or '—'}/{it.get('priority') or '—'} | "
                    f"{Path(wtp).name if wtp else '—'} | "
                    f"{'#'+str(pr['number']) if pr else '—'} | "
                    f"{_ci(pr['statusCheckRollup']) if pr else '—'} |")
    STATUS_MD.write_text("\n".join(rows) + "\n")
    print(f"status -> {STATUS_MD}", flush=True)


def run_iteration():
    print("=== iteration: running claude (opus) ===", flush=True)
    subprocess.run(
        ["claude", "-p", build_instruction(status_opts()), "--model", MODEL,
         "--permission-mode", "auto"],
        env=env())


def wait_for_trigger(seen, wake_mtime):
    """Block until a GitHub change, a wake-file touch, or MAX_WAIT. Returns reason."""
    deadline = time.time() + MAX_WAIT
    while time.time() < deadline:
        time.sleep(POLL)
        if WAKE.exists() and WAKE.stat().st_mtime != wake_mtime:
            return "process-done"
        if gh_fingerprint() != seen:
            return "github-update"
    return "1h-timer"


def main():
    STATE.mkdir(parents=True, exist_ok=True)
    acquire_lock()
    WAKE.touch()
    while True:
        run_iteration()
        write_status()
        seen, wake_mtime = gh_fingerprint(), WAKE.stat().st_mtime  # re-baseline after our run
        reason = wait_for_trigger(seen, wake_mtime)
        write_status()                                             # refresh on every wake
        print(f"=== trigger: {reason} ===", flush=True)


def selftest():
    e = env()
    assert e["OTEL_RESOURCE_ATTRIBUTES"] == "usage_mode=ralph,agent_type=PM"
    assert e["RALPH_WAKE"].endswith("wake")
    assert _ci([]) == "—" and _ci([{"conclusion": "SUCCESS"}]) == "pass"
    assert _ci([{"conclusion": "SUCCESS"}, {"status": "IN_PROGRESS"}]) == "pending"
    assert _ci([{"conclusion": "FAILURE"}]) == "fail"
    a = {"issue1": "t0", "pr2": "t0"}
    assert a == dict(a) and a != {**a, "issue1": "t1"}  # change detection is dict-inequality
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


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
