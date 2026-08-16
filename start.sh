#!/usr/bin/env bash

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_PYTHON="${ENGINE_PYTHON:-${SCRIPT_DIR}/.trading/bin/python}"

if [[ -f "${SCRIPT_DIR}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}/.env"
    set +a
fi

usage() {
    cat <<'EOF'
Usage: ./start.sh <engine> [engine arguments]

Engines:
  strategy             Start the strategy engine
  position             Start the position engine
  risk                 Start the risk engine
  trade                Start the trade engine
  position-projector   Start the position view projector
  all                  Start all engines

Examples:
  ./start.sh position
  ./start.sh strategy --once --symbol BTCUSDT
  ./start.sh all
EOF
}

start_engine() {
    local engine="$1"
    shift

    local log_name="$engine"
    if [[ "$engine" == "position-projector" ]]; then
        log_name="projector"
    fi

    nohup "$ENGINE_PYTHON" -m trading_engine "$engine" -- "$@" \
        > "${SCRIPT_DIR}/nohup-${log_name}.log" 2>&1 &
    echo "Started ${engine} (pid=$!, log=nohup-${log_name}.log)"
}

if [[ ! -x "$ENGINE_PYTHON" ]]; then
    echo "Python executable not found or not executable: ${ENGINE_PYTHON}" >&2
    exit 1
fi

if [[ $# -eq 0 ]]; then
    usage
    exit 2
fi

engine="$1"
shift

case "$engine" in
    strategy)
        if [[ $# -eq 0 ]]; then
            set -- --stream --interval-seconds 1
        fi
        start_engine strategy "$@"
        ;;
    position|risk|trade)
        start_engine "$engine" "$@"
        ;;
    position-projector|projector)
        if [[ $# -eq 0 ]]; then
            set -- --stream --interval-seconds 1
        fi
        start_engine position-projector "$@"
        ;;
    all)
        if [[ $# -ne 0 ]]; then
            echo "The 'all' command does not accept engine-specific arguments." >&2
            usage >&2
            exit 2
        fi
        start_engine position
        start_engine strategy --stream --interval-seconds 1
        start_engine risk
        start_engine trade
        start_engine position-projector --stream --interval-seconds 1
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
