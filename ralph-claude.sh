#!/bin/sh
# ralph-claude: run `claude` always tagged usage_mode=ralph for OTEL.
# First arg = agent_type (PM|implementer|validator); rest passed to claude.
# Hard-sets the resource attrs so a ~/.zshrc `usage_mode=interactive` default
# (re-sourced by Claude Code's Bash-tool shell snapshot) can't leak in.
# ponytail: env is hard-coded here, single source of truth; edit if endpoint moves.
at="${1:?agent_type required}"; shift
exec env \
  CLAUDE_CODE_ENABLE_TELEMETRY=1 \
  OTEL_METRICS_EXPORTER=otlp \
  OTEL_EXPORTER_OTLP_PROTOCOL=grpc \
  OTEL_EXPORTER_OTLP_ENDPOINT=http://192.168.11.155:4317 \
  OTEL_RESOURCE_ATTRIBUTES="usage_mode=ralph,agent_type=${at}" \
  claude "$@"
