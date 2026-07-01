#!/bin/bash
# Two-agent Ralph loop.
#
# Each iteration runs two agents back-to-back:
#   IMPLEMENTER — picks one story, builds it, signals READY_FOR_VERIFICATION.
#   VERIFIER    — fresh context, adversarial, checks each acceptance criterion
#                 independently, then sets passes:true or leaves feedback.
#
# Usage: ./ralph.sh [max-iterations]
set -e

MAX_ITERATIONS=${1:-10}
LOCK_FILE="$PWD/ralph.lock"

cleanup() {
  rm -f "$LOCK_FILE"
}
echo $OTEL_RESOURCE_ATTRIBUTES
if [ -f "$LOCK_FILE" ]; then
  echo "⚠️  ralph.lock exists (PID $(cat "$LOCK_FILE")). Ralph may already be running."
  echo "   Delete $LOCK_FILE to force-start."
  exit 1
fi

echo $$ > "$LOCK_FILE"
trap cleanup EXIT   # register only after we own the lock, so the bail-out above doesn't delete someone else's

echo "🚀 Starting Ralph (two-agent mode, max $MAX_ITERATIONS iterations)"

wait_for_quota_reset() {
  local output="$1"
  local reset_time tz reset_epoch now_epoch wait_secs
  reset_time=$(echo "$output" | grep -oP '(?<=resets\s)\d+:\d+[ap]m' | head -1)
  tz=$(echo "$output" | grep -oP '\(([^)]+)\)' | tr -d '()' | head -1)
  if [ -n "$reset_time" ] && [ -n "$tz" ]; then
    reset_epoch=$(TZ="$tz" date -d "$reset_time" +%s 2>/dev/null || true)
    now_epoch=$(date +%s)
    if [ -n "$reset_epoch" ] && [ "$reset_epoch" -gt "$now_epoch" ]; then
      wait_secs=$(( reset_epoch - now_epoch + 30 ))
      echo "⏳ Quota hit. Resets at $reset_time ($tz). Waiting ${wait_secs}s..."
      sleep "$wait_secs"
      return
    fi
  fi
  echo "⏳ Quota hit (could not parse reset time). Waiting 1 hour..."
  sleep 3600
}

# ── IMPLEMENTER prompt ────────────────────────────────────────────────────────
# Reads: prd.json, progress.txt, CLAUDE.md (project conventions)
# Must NOT set passes:true — signals READY_FOR_VERIFICATION instead.
IMPLEMENTER_PROMPT='@prd.json @progress.txt @CLAUDE.md

You are the IMPLEMENTER agent. Work on exactly ONE story per run.

1. Check you are on the correct branch. If on main/master and no feature branch
   exists, create one.

2. Pick the highest-priority story where passes:false.

3. Before writing any code, read the story'"'"'s acceptance criteria and write
   one sentence per criterion on how you will verify it is met.

4. Implement the story. Work ONLY on this story.

5. Run the project'"'"'s test suite (see CLAUDE.md for the exact command).
   All tests must pass before continuing.

6. Inspect the output as a first-time human reader would:
   - If CLAUDE.md describes a dry-run, fixture, or preview mode, use it to
     generate the actual output and look at it.
   - Ask: does anything look repeated, broken, or structurally wrong?
   - Ask: would a human immediately notice something is off?
   Fix anything obvious before signalling.

7. Commit: feat: [ID] - [Title]

8. Append to progress.txt and signal readiness:
   ## [Date] - [Story ID] - [Title]
   ### What was implemented
   - ...
   ### Files changed
   - ...
   ### Learnings
   - ...
   READY_FOR_VERIFICATION: [Story ID]

DO NOT set passes:true — that is the verifier'"'"'s job.
DO NOT work on more than one story.

## Stop Condition

If ALL stories already have passes:true, create a PR using the GitHub CLI
linking to relevant issues, then output:
<promise>COMPLETE</promise>'

# ── VERIFIER prompt ───────────────────────────────────────────────────────────
# Fresh context: reads prd.json and progress.txt but has NOT seen the
# implementation being written. Adversarial by design.
VERIFIER_PROMPT='@prd.json @progress.txt @CLAUDE.md

You are the VERIFIER agent. You have NOT seen the implementation being written.
Your job is to find problems, not confirm success. Be adversarial.

1. Find the story marked READY_FOR_VERIFICATION in progress.txt.
   Read its acceptance criteria in prd.json.

2. For EACH acceptance criterion, independently verify it is met:
   CRITERION: [criterion text]
   VERDICT: PASS / FAIL — [one sentence reason]

3. Run the project'"'"'s test suite independently (see CLAUDE.md).

4. If CLAUDE.md describes a dry-run, fixture, or preview mode, generate the
   output and inspect it as a first-time human reader with no codebase knowledge:
   - Does the output look correct?
   - Does anything appear repeated or duplicated that should not be?
   - Are there structural anomalies a human would immediately notice?
   - If the output contains a list of items or embeds, are IDs/values unique?

5. If ALL criteria pass AND the output looks correct to a human reader:
   - Set passes:true for the story in prd.json
   - Append to progress.txt:
     VERIFIED_PASS: [Story ID] — [one sentence: what was checked]
   - Commit: chore: verify [ID] passes acceptance criteria
   - Output: <promise>VERIFIED_PASS</promise>

6. If ANYTHING fails:
   - Leave passes:false
   - Append to progress.txt:
     VERIFIED_FAIL: [Story ID] — [specific failure: which criterion, what was wrong]
   - Do NOT commit
   - Output: <promise>VERIFIED_FAIL</promise>'

# ── main loop ─────────────────────────────────────────────────────────────────
i=1
while [ "$i" -le "$MAX_ITERATIONS" ]; do
  echo ""
  echo "═══ Iteration $i — IMPLEMENTER ═══"

  IMPL_OUTPUT=$(/data/dev/skills/ralph-claude.sh implementer --permission-mode auto --model sonnet -p "$IMPLEMENTER_PROMPT" \
    2>&1 | tee /dev/stderr) || true

  if echo "$IMPL_OUTPUT" | grep -qE "hit your (session )?limit"; then
    wait_for_quota_reset "$IMPL_OUTPUT"
    continue
  fi

  if echo "$IMPL_OUTPUT" | grep -q "<promise>COMPLETE</promise>"; then
    echo "✅ All stories complete!"
    exit 0
  fi

  if ! echo "$IMPL_OUTPUT" | grep -q "READY_FOR_VERIFICATION"; then
    echo "⚠️  Implementer did not signal READY_FOR_VERIFICATION — retrying"
    (( i++ ))
    continue
  fi

  echo ""
  echo "═══ Iteration $i — VERIFIER ═══"

  VERIFY_OUTPUT=$(/data/dev/skills/ralph-claude.sh validator --permission-mode auto --model sonnet -p "$VERIFIER_PROMPT" \
    2>&1 | tee /dev/stderr) || true

  if echo "$VERIFY_OUTPUT" | grep -qE "hit your (session )?limit"; then
    wait_for_quota_reset "$VERIFY_OUTPUT"
    continue  # retry verification for same story without incrementing
  fi

  if echo "$VERIFY_OUTPUT" | grep -q "<promise>VERIFIED_PASS</promise>"; then
    echo "✅ Story verified."
  else
    echo "⚠️  Verification failed — next iteration will re-implement."
  fi

  sleep 2
  (( i++ ))
done

echo "⚠️ Max iterations reached"
exit 1
