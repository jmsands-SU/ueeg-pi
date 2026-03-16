#!/bin/bash
# /usr/local/bin/sdr_monitor.sh

set -e

LOG_FILE="/var/log/sdr_monitor.log"
SCRIPT_PATH="/home/pi/pythonreadersinglechannel.py"
CREDENTIALS_FILE="/home/pi/ueegproject-aea2731f9c3a.json"
MAX_RESTART_ATTEMPTS=3
RESTART_DELAY=30
GCS_RETRY_DELAY=60
HEALTH_CHECK_INTERVAL=60
SDR_CHECK_INTERVAL=300  # Check for SDR every 5 minutes if missing

log() {
    echo "[$$(date '+%Y-%m-%d %H:%M:%S')] \$1" | tee -a "$$LOG_FILE"
}

check_sdr_connected() {
    # Check for USB SDR device
    # Adjust the grep pattern based on your SDR hardware
    # Common patterns:
    # - "RTL" for RTL-SDR dongles
    # - "Software Defined Radio" 
    # - Check by USB vendor/product ID
    
    # Method 1: Check by lsusb (most common)
    if lsusb | grep -iqE "RTL|SDR|Software Defined Radio|0bda:2838"; then
        log "SDR device detected via USB"
        return 0
    fi
    
    # Method 2: Check for SPI device (if using SPI-based SDR)
    if [ -e /dev/spidev0.0 ] || [ -e /dev/spidev0.1 ]; then
        log "SPI device detected (potential SDR interface)"
        return 0
    fi
    
    # Method 3: Check for specific device file your SDR creates
    # Uncomment and modify if your SDR creates a specific device
    # if [ -e /dev/your_sdr_device ]; then
    #     log "SDR device file detected"
    #     return 0
    # fi
    
    log "No SDR device detected"
    return 1
}

wait_for_sdr() {
    local max_wait=300  # 5 minutes
    local waited=0
    
    log "Waiting for SDR device to be connected..."
    
    while [ $$waited -lt $$max_wait ]; do
        if check_sdr_connected; then
            log "SDR device found!"
            return 0
        fi
        sleep 10
        waited=$((waited + 10))
        log "Still waiting for SDR... ($$waited/$$max_wait seconds)"
    done
    
    log "SDR wait timeout - will continue checking periodically"
    return 1
}

check_gcs_auth() {
    if [ ! -f "$CREDENTIALS_FILE" ]; then
        log "ERROR: GCS credentials file not found: $CREDENTIALS_FILE"
        return 1
    fi
    
    export GOOGLE_APPLICATION_CREDENTIALS="$CREDENTIALS_FILE"
    
    if python3 -c "
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
    
    while [ $$waited -lt $$max_wait ]; do
        if check_network; then
            return 0
        fi
        sleep 10
        waited=$((waited + 10))
        log "Still waiting for network... ($$waited/$$max_wait seconds)"
    done
    
    log "Network wait timeout"
    return 1
}

start_python_script() {
    log "Starting Python SDR script..."
    
    export GOOGLE_APPLICATION_CREDENTIALS="$CREDENTIALS_FILE"
    
    python3 "$$SCRIPT_PATH" >> "$$LOG_FILE" 2>&1 &
    local pid=$!
    
    echo $pid > /var/run/sdr_script.pid
    log "Python script started with PID: $pid"
    
    return 0
}

stop_python_script() {
    if [ -f /var/run/sdr_script.pid ]; then
        local pid=$(cat /var/run/sdr_script.pid)
        if kill -0 $pid 2>/dev/null; then
            log "Stopping Python script (PID: $pid)..."
            kill $pid
            sleep 5
            
            if kill -0 $pid 2>/dev/null; then
                log "Force killing Python script..."
                kill -9 $pid
            fi
        fi
        rm -f /var/run/sdr_script.pid
    fi
}

check_script
