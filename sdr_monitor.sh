#!/bin/bash
# /usr/local/bin/sdr_monitor.sh

LOG_FILE="/var/log/sdr_monitor.log"
SCRIPT_PATH="/home/ueeg/ueeg-pi/sdr_reader_gcs_write.py"
CREDENTIALS_FILE="/home/ueeg/ueeg-pi/ueegproject-aea2731f9c3a.json"
VENV_PATH="/home/ueeg/sdr_venv"
BOARD_CONFIG_FILE="/home/ueeg/ueeg-pi/board_config.json"
RBF_FILE=$("$VENV_PATH/bin/python3" -c "import json; cfg=json.load(open('$BOARD_CONFIG_FILE')); print(cfg['rbf_file'])" 2>/dev/null || echo "")
STATUS_FILE="/tmp/sdr_monitor_status.json"
MAX_RESTART_ATTEMPTS=3
RESTART_DELAY=10
GCS_RETRY_DELAY=60
HEALTH_CHECK_INTERVAL=60
SDR_CHECK_INTERVAL=300
ENABLE_GCS_LOGGING=false
GCS_LOG_NAME="sdr-monitor"

LOCKFILE="/var/lock/sdr-monitor.lock"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log_to_gcs() {
    local message="$1"
    local severity="${2:-INFO}"
    local labels="${3:-}"
    
    if [ "$ENABLE_GCS_LOGGING" != "true" ] || [ ! -f "$CREDENTIALS_FILE" ]; then
        return 0
    fi
    
    export GOOGLE_APPLICATION_CREDENTIALS="$CREDENTIALS_FILE"
    
    # Run in background so it doesn't block
    (
    "$VENV_PATH/bin/python3" -c "
from google.cloud import logging_v2
import socket
import datetime
import json

try:
    client = logging_v2.Client()
    logger = client.logger('$GCS_LOG_NAME')
    
    log_data = {
        'message': '''$message''',
        'hostname': socket.gethostname(),
        'service': 'sdr-monitor',
        'timestamp': datetime.datetime.utcnow().isoformat()
    }
    
    labels = '$labels'
    if labels:
        try:
            label_dict = json.loads(labels)
            log_data.update(label_dict)
        except:
            pass
    
    logger.log_struct(log_data, severity='$severity')
    
except Exception as e:
    pass
" 2>&1 | logger -t sdr-monitor-gcs
    ) &
}

update_status() {
    local status="$1"
    local details="${2:-}"
    cat > "$STATUS_FILE" << EOF
{
    "status": "$status",
    "details": "$details",
    "timestamp": "$(date -Iseconds)",
    "pid": $$,
    "hostname": "$(hostname)",
    "script_running": ${SCRIPT_RUNNING:-false}
}
EOF
}

check_sdr_connected() {
    # Method 1: Check using bladeRF-cli
    if command -v bladeRF-cli &> /dev/null; then
        if bladeRF-cli -p 2>/dev/null | grep -q "bladeRF"; then
            return 0
        fi
    fi
    
    # Method 2: Check via lsusb for BladeRF USB IDs
    if lsusb | grep -iE "2cf0:5246|1d50:6066|Nuand"; then
        return 0
    fi
    
    # Method 3: Check for BladeRF device files
    if ls /dev/bladerf* 2>/dev/null; then
        return 0
    fi
    
    return 1
}

check_gcs_auth() {
    if [ ! -f "$CREDENTIALS_FILE" ]; then
        log "ERROR: GCS credentials file not found: $CREDENTIALS_FILE"
        return 1
    fi
    
    export GOOGLE_APPLICATION_CREDENTIALS="$CREDENTIALS_FILE"
    
    if "$VENV_PATH/bin/python3" -c "
from google.cloud import storage
import sys
try:
    client = storage.Client()
    list(client.list_buckets(max_results=1))
    sys.exit(0)
except Exception as e:
    print(f'GCS auth failed: {e}')
    sys.exit(1)
" 2>&1; then
        log "GCS authentication successful"
        return 0
    else
        log "GCS authentication failed"
        return 1
    fi
}

check_network() {
    if ping -c 3 -W 5 8.8.8.8 >/dev/null 2>&1; then
        log "Network connectivity confirmed"
        return 0
    else
        log "No network connectivity"
        return 1
    fi
}

wait_for_network() {
    local max_wait=300
    local waited=0
    
    log "Waiting for network connectivity..."
    
    while [ $waited -lt $max_wait ]; do
        if check_network; then
            return 0
        fi
        sleep 10
        waited=$((waited + 10))
        log "Still waiting for network... ($waited/$max_wait seconds)"
    done
    
    log "Network wait timeout"
    return 1
}

start_python_script() {
    log "Starting Python SDR script with venv..."
    
    # Check if already running
    if pgrep -f "$VENV_PATH/bin/python3.*" > /dev/null; then
        log "ERROR: Python script already running! Not starting another instance."
        local existing_pid=$(pgrep -f "$VENV_PATH/bin/python3.*sdr_reader_gcs_write")
        log "Existing PID: $existing_pid"
        echo $existing_pid > /tmp/sdr_script.pid
        return 0
    fi
    
    # Load FPGA
    log "Loading FPGA..."

    bladeRF-cli -l "$RBF_FILE" 2>&1 | tee -a "$LOG_FILE" || log "FPGA load had non-zero exit"

    
    log "Successfully loaded FPGA bitstream!"
    sleep 3
    
    export GOOGLE_APPLICATION_CREDENTIALS="$CREDENTIALS_FILE"
    export PYTHONUNBUFFERED=1
    
    cd /home/ueeg || {
        log "ERROR: Cannot cd to /home/ueeg"
        return 1
    }
    
    log "Launching Python..."
    
    set +e
    PYTHONUNBUFFERED=1 \
    GOOGLE_APPLICATION_CREDENTIALS="$CREDENTIALS_FILE" \
    "$VENV_PATH/bin/python3" -u "$SCRIPT_PATH" >> "$LOG_FILE" 2>&1 &
    local bg_pid=$!
    set -e
    
    log "Python launched with background PID: $bg_pid"
    
    sleep 5
    
    local python_pid=$(pgrep -f "$VENV_PATH/bin/python3.*sdr_reader_gcs_write")
    
    if [ -n "$python_pid" ]; then
        echo $python_pid > /tmp/sdr_script.pid
        log "Python daemon running with PID: $python_pid"
        return 0
    else
        log "WARNING: No Python process found but not treating as fatal"
        return 0
    fi
}

stop_python_script() {
    log "Stopping Python script..."
    
    # Find all Python processes running your script
    local pids=$(ps aux | grep "[p]ythonreadersinglechannel.py" | awk '{print $2}')
    
    if [ -n "$pids" ]; then
        for pid in $pids; do
            log "Stopping Python process PID: $pid"
            kill $pid 2>/dev/null || true
        done
        
        sleep 5
        
        # Force kill if still running
        pids=$(ps aux | grep "[p]ythonreadersinglechannel.py" | awk '{print $2}')
        if [ -n "$pids" ]; then
            log "Force killing remaining processes..."
            for pid in $pids; do
                kill -9 $pid 2>/dev/null || true
            done
        fi
    else
        log "No Python script running"
    fi
    
    rm -f /tmp/sdr_script.pid
}

check_script_health() {
    # Look for Python running from our venv
    local running_pid=$(pgrep -f "$VENV_PATH/bin/python3.*sdr_reader_gcs_write")
    
    if [ -n "$running_pid" ]; then
        echo $running_pid > /tmp/sdr_script.pid
        return 0
    fi
    
    log "Python script not running (no python3 process found in venv)"
    return 1
}

restart_script() {
    log "Restarting SDR script..."
    
    stop_python_script
    sleep $RESTART_DELAY
    
    if ! check_sdr_connected; then
        log "Cannot restart - BladeRF not connected"
        return 1
    fi
    
    if ! wait_for_network; then
        log "Cannot restart - no network"
        return 1
    fi
    
    local retry_count=0
    while [ $retry_count -lt $MAX_RESTART_ATTEMPTS ]; do
        if check_gcs_auth; then
            start_python_script
            return 0
        else
            retry_count=$((retry_count + 1))
            log "GCS auth failed (attempt $retry_count/$MAX_RESTART_ATTEMPTS), waiting $GCS_RETRY_DELAY seconds..."
            sleep $GCS_RETRY_DELAY
        fi
    done
    
    log "Failed to authenticate with GCS after $MAX_RESTART_ATTEMPTS attempts"
    return 1
}

# Check for existing instance
if [ -f "$LOCKFILE" ]; then
    LOCK_PID=$(cat "$LOCKFILE")
    if kill -0 "$LOCK_PID" 2>/dev/null; then
        log "ERROR: Another instance already running (PID: $LOCK_PID)"
        exit 1
    else
        log "Removing stale lockfile (PID $LOCK_PID is dead)"
        rm -f "$LOCKFILE"
    fi
fi

# Create lockfile
echo $$ > "$LOCKFILE"
trap "rm -f $LOCKFILE" EXIT

# Headless boot delay
UPTIME_SECONDS=$(awk '{print int($1)}' /proc/uptime)
if [ $UPTIME_SECONDS -lt 120 ]; then
    log "Recent boot detected (uptime: ${UPTIME_SECONDS}s) - waiting 30s..."
    sleep 30
fi

log "=========================================="
log "SDR monitor started (PID: $$)"
log "Lockfile: $LOCKFILE"
log "Using virtual environment: $VENV_PATH"
log "GCS Logging: $ENABLE_GCS_LOGGING"
log "=========================================="

update_status "initializing" "Checking for virtual environment"

if [ -z "$RBF_FILE" ]; then
    log "ERROR: Could not read rbf_file from $BOARD_CONFIG_FILE"
    update_status "error" "board_config.json missing or rbf_file not set"
    exit 1
fi
log "RBF file: $RBF_FILE"

if [ ! -d "$VENV_PATH" ]; then
    log "ERROR: Virtual environment not found at $VENV_PATH"
    update_status "error" "Virtual environment not found"
    exit 1
fi

if [ ! -f "$VENV_PATH/bin/python3" ]; then
    log "ERROR: Python interpreter not found in venv"
    update_status "error" "Python interpreter not found in venv"
    exit 1
fi

update_status "waiting_for_network" "Checking network connectivity"

if ! wait_for_network; then
    log "FATAL: No network available at startup"
    update_status "error" "No network available"
    exit 1
fi

update_status "checking_gcs" "Authenticating with Google Cloud"

if ! check_gcs_auth; then
    log "FATAL: GCS authentication failed at startup"
    update_status "error" "GCS authentication failed"
    exit 1
fi

# Start script only if BladeRF is connected
if check_sdr_connected; then
    update_status "starting_script" "Launching SDR data collection"
    
    if start_python_script; then
        log "start_python_script returned success"
        SCRIPT_RUNNING=true
    else
        log "ERROR: start_python_script returned failure but continuing anyway"
        SCRIPT_RUNNING=false
    fi
    
    update_status "running" "SDR script active, collecting data"
else
    log "Waiting for BladeRF before starting script"
    update_status "waiting_for_bladerf" "Ready but waiting for hardware"
    SCRIPT_RUNNING=false
fi

# Main monitoring loop
LOOP_COUNT=0

while true; do
    sleep $HEALTH_CHECK_INTERVAL
    LOOP_COUNT=$((LOOP_COUNT + 1))
    
    # Wrap everything in error handling
    {
        # Check if BladeRF disconnected
        if ! check_sdr_connected; then
            log "ERROR: BladeRF disconnected!"
            
            if [ "$SCRIPT_RUNNING" = true ]; then
                log "Stopping script due to missing BladeRF"
                stop_python_script || true
                SCRIPT_RUNNING=false
                update_status "bladerf_disconnected" "Hardware removed" || true
            fi
            
            log "Waiting for BladeRF to reconnect..."
            sleep 10
            continue
        fi
        
        # If BladeRF reconnected, restart script
        if [ "$SCRIPT_RUNNING" = false ]; then
            log "BladeRF reconnected! Reloading FPGA and restarting..."
            

            bladeRF-cli -l "$RBF_FILE" 2>&1 | tee -a "$LOG_FILE" || true
            sleep 3
            
            if restart_script; then
                SCRIPT_RUNNING=true
                update_status "running" "Recovered from disconnect" || true
            fi
            continue
        fi
        
        # Normal health check
        if ! check_script_health; then
            log "Health check failed - script not running"
            restart_script || true
        else
            log "Health check passed"
        fi
        
    } || {
        log "ERROR in main loop, but continuing..."
    }
done
