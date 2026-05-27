#!/usr/bin/env bash
# =============================================================================
# pipeline-entrypoint.sh — subcommand dispatcher for the pipeline image
# =============================================================================
# Convenience wrapper that maps a short subcommand to the underlying Python
# script. The verbose form
#
#     docker compose run --rm pipeline python scripts/run_pipeline.py --mode incremental
#
# and the dispatcher form
#
#     docker compose run --rm pipeline pipeline --mode incremental
#
# are equivalent. The dispatcher exists for ergonomics and as a discoverability
# aid; production scripts and CI can use whichever form they prefer.
# =============================================================================
set -euo pipefail

# No args → fall through to default CMD (pipeline --mode incremental).
if [ "$#" -eq 0 ]; then
    exec python scripts/run_pipeline.py --mode incremental
fi

case "$1" in
    pipeline)
        shift
        exec python scripts/run_pipeline.py "$@"
        ;;
    migrate)
        shift
        exec python scripts/migrate.py "$@"
        ;;
    inspect)
        shift
        exec python scripts/inspect_incremental_load.py "$@"
        ;;
    inspect-api)
        shift
        exec python scripts/inspect_api_output.py "$@"
        ;;
    shell)
        # Interactive shell shorthand. For non-interactive bash with args,
        # use `bash -c "..."` (handled below).
        exec /bin/bash
        ;;
    bash|sh|python|pytest)
        # Direct interpreter/tool invocation. Preserves any args after $1.
        # Examples:
        #   docker compose run --rm pipeline bash -c "whoami && id"
        #   docker compose run --rm pipeline python -m pytest tests/
        exec "$@"
        ;;
    *)
        # Anything else: forward unchanged to run_pipeline.py. Preserves the
        # default-CMD shape (`pipeline --mode incremental` → flags only).
        # To bypass the dispatcher entirely, use:
        #   docker compose run --rm --entrypoint /bin/bash pipeline -c "..."
        exec python scripts/run_pipeline.py "$@"
        ;;
esac
