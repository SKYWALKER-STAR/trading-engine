#!/usr/bin/env bash

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_PYTHON="${ENGINE_PYTHON:-${SCRIPT_DIR}/.trading/bin/python}"

usage() {
    cat <<'EOF'
Usage: ./stop.sh <engine>

Engines:
  strategy               Stop the strategy engine
  position               Stop the position engine
  risk                   Stop the risk engine
  trade                  Stop the trade engine
  position-projector     Stop the position view projector
  position-debug-dashboard Stop the browser dashboard for tracking position transitions
  all                    Stop all engines

Examples:
  ./stop.sh risk
  ./stop.sh position-projector
  ./stop.sh all
EOF
}

get_pids_for_engine() {
    local engine="$1"

    # Match the exact command shape used in start.sh:
    # <python> -m trading_engine <engine> -- ...
    ps -eo pid=,args= | awk -v py="$ENGINE_PYTHON" -v eng="$engine" '
        index($0, py " -m trading_engine " eng " ") > 0 { print $1; next }
        index($0, py " -m trading_engine " eng) > 0 && $0 ~ (" " eng "$") { print $1 }
    '
}

stop_pids() {
    local engine="$1"
    local pids

    pids="$(get_pids_for_engine "$engine")"
    if [[ -z "$pids" ]]; then
        echo "No running process found for ${engine}."
        return 0
    fi

    echo "Stopping ${engine}: ${pids}"
    # shellcheck disable=SC2086
    kill ${pids}

    local waited=0
    while [[ $waited -lt 5 ]]; do
        local remaining
        remaining=""
        for pid in $pids; do
            if kill -0 "$pid" 2>/dev/null; then
                remaining+="${pid} "
            fi
        done

        if [[ -z "$remaining" ]]; then
            echo "Stopped ${engine}."
            return 0
        fi

        sleep 1
        waited=$((waited + 1))
    done

    echo "Force killing ${engine}: ${remaining}"
    # shellcheck disable=SC2086
    kill -9 ${remaining}
}

if [[ $# -eq 0 ]]; then
    usage
    exit 2
fi

engine="$1"

case "$engine" in
    strategy|position|risk|trade|position-debug-dashboard)
        stop_pids "$engine"
        ;;
    position-projector|projector)
        stop_pids position-projector
        ;;
    all)
        stop_pids position
        stop_pids strategy
        stop_pids risk
        stop_pids trade
        stop_pids position-projector
        stop_pids position-debug-dashboard
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "Unknown engine: ${engine}" >&2
        usage >&2
        exit 2
        ;;
esac
