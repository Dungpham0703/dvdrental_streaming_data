#!/bin/bash

# ==============================================================================
# DVD Rental - Continuous Stress Test Runner for Linux (EC2)
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

LOG_FILE="$SCRIPT_DIR/stress_test_monitor.log"

echo "========================================================" | tee -a "$LOG_FILE"
echo "Starting Continuous Stress Test Runner" | tee -a "$LOG_FILE"
echo "Date: $(date)" | tee -a "$LOG_FILE"
echo "========================================================" | tee -a "$LOG_FILE"

while true; do
    echo "[$(date)] Launching stress_test.py..." | tee -a "$LOG_FILE"

    python3 -u stress_test.py

    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "[$(date)] Stress test finished normally (Target DB size reached). Exiting runner." | tee -a "$LOG_FILE"
        break
    else
        echo "[$(date)] WARNING: Stress test crashed with exit code $EXIT_CODE." | tee -a "$LOG_FILE"
        echo "[$(date)] Restarting in 10 seconds to ensure continuous load..." | tee -a "$LOG_FILE"
        sleep 10
    fi
done

echo "Runner stopped." | tee -a "$LOG_FILE"
