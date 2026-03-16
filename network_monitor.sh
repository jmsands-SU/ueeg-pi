#!/bin/bash
# /usr/local/bin/network_monitor.sh - Priority-Based Version with Optional Cellular

set -e

LOG_FILE="/var/log/network_monitor.log"
WIFI_CHECK_INTERVAL=30
WIFI_QUALITY_THRESHOLD=50
PING_TIMEOUT=5
PING_COUNT=3

# Sixfab ECM configuration
SIXFAB_INTERFACE="usb0"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

check_wifi_quality() {
    local quality=$(iwconfig wlan0 2>/dev/null | grep -i quality | awk -F'=' '{print $2}' | awk '{print $1}' | cut -d'/' -f1)
    local max=$(iwconfig wlan0 2>/dev/null | grep -i quality | awk -F'=' '{print $2}' | awk '{print $1}' | cut -d'/' -f2)
    
    if [ -z "$quality" ] || [ -z "$max" ]; then
        echo "0"
        return
    fi
    
    echo $(( quality * 100 / max ))
}

check_connectivity() {
    if ping -c $PING_COUNT -W $PING_TIMEOUT 8.8.8.8 >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

get_primary_interface() {
    ip route show default | head -1 | grep -oP 'dev \K\S+'
}

is_interface_up() {
    local interface=$1
    if ip link show "$interface" 2>/dev/null | grep -q "state UP"; then
        return 0
    fi
    return 1
}

has_ip_address() {
    local interface=$1
    if ip addr show "$interface" 2>/dev/null | grep -q "inet "; then
        return 0
    fi
    return 1
}

cellular_hardware_present() {
    if ip link show "$SIXFAB_INTERFACE" &>/dev/null; then
        return 0
    fi
    return 1
}

is_cellular_connected() {
    if ! cellular_hardware_present; then
        return 1
    fi
    
    if ip addr show "$SIXFAB_INTERFACE" 2>/dev/null | grep -q "inet "; then
        return 0
    fi
    return 1
}

ensure_btberry_running() {
    if systemctl list-unit-files | grep -q "btberry-wifi"; then
        if ! systemctl is-active --quiet btberry-wifi; then
            log "Starting btberry-wifi (Bluetooth service)..."
            sudo systemctl start btberry-wifi
        fi
    fi
}

start_cellular() {
    log "Starting Sixfab cellular connection..."
    
    if ! cellular_hardware_present; then
        log "Cellular interface $SIXFAB_INTERFACE not found - hardware not connected"
        return 1
    fi
    
    sudo ip link set "$SIXFAB_INTERFACE" up
    
    if command -v dhcpcd &> /dev/null; then
        log "Requesting DHCP on $SIXFAB_INTERFACE..."
        sudo dhcpcd "$SIXFAB_INTERFACE"
    elif command -v dhclient &> /dev/null; then
        log "Requesting DHCP on $SIXFAB_INTERFACE..."
        sudo dhclient "$SIXFAB_INTERFACE"
    fi
    
    sleep 10
    
    if ip addr show "$SIXFAB_INTERFACE" | grep -q "inet "; then
        local ip=$(ip -4 addr show "$SIXFAB_INTERFACE" | grep inet | awk '{print $2}')
        log "ECM interface acquired IP: $ip"
        return 0
    else
        log "Failed to acquire IP on cellular interface"
        return 1
    fi
}

stop_cellular() {
    log "Stopping Sixfab cellular connection..."
    
    if ! cellular_hardware_present; then
        return 0
    fi
    
    if command -v dhcpcd &> /dev/null; then
        sudo dhcpcd -k "$SIXFAB_INTERFACE" 2>/dev/null || true
    elif command -v dhclient &> /dev/null; then
        sudo dhclient -r "$SIXFAB_INTERFACE" 2>/dev/null || true
    fi
    
    sudo ip link set "$SIXFAB_INTERFACE" down 2>/dev/null || true
}

ensure_cellular_running() {
    if ! cellular_hardware_present; then
        return 0
    fi
    
    if ! is_interface_up "$SIXFAB_INTERFACE"; then
        log "Bringing up cellular interface..."
        sudo ip link set "$SIXFAB_INTERFACE" up
        sleep 2
    fi
    
    if ! has_ip_address "$SIXFAB_INTERFACE"; then
        log "Getting IP for cellular interface..."
        if command -v dhcpcd &> /dev/null; then
            sudo dhcpcd "$SIXFAB_INTERFACE" &
        elif command -v dhclient &> /dev/null; then
            sudo dhclient "$SIXFAB_INTERFACE" &
        fi
        sleep 10
    fi
}

ensure_wifi_running() {
    if ! is_interface_up "wlan0"; then
        log "Bringing up WiFi interface..."
        sudo ip link set wlan0 up
        sudo wpa_cli -i wlan0 reconnect
    fi
}

set_wifi_priority() {
    log "Setting WiFi as primary connection..."
    
    sudo ip route del default 2>/dev/null || true
    
    local wifi_gw=$(ip route show dev wlan0 | grep -oP 'via \K\S+' | head -1)
    if [ -n "$wifi_gw" ]; then
        sudo ip route add default via "$wifi_gw" dev wlan0 metric 100
    fi
    
    if cellular_hardware_present && has_ip_address "$SIXFAB_INTERFACE"; then
        local cell_gw=$(ip route show dev "$SIXFAB_INTERFACE" | grep -oP 'via \K\S+' | head -1)
        if [ -n "$cell_gw" ]; then
            sudo ip route add default via "$cell_gw" dev "$SIXFAB_INTERFACE" metric 200 2>/dev/null || true
        fi
    fi
}

set_cellular_priority() {
    log "Setting cellular as primary connection..."
    
    if ! cellular_hardware_present; then
        log "Cannot set cellular priority - hardware not present"
        return 1
    fi
    
    sudo ip route del default 2>/dev/null || true
    
    local cell_gw=$(ip route show dev "$SIXFAB_INTERFACE" | grep -oP 'via \K\S+' | head -1)
    if [ -n "$cell_gw" ]; then
        sudo ip route add default via "$cell_gw" dev "$SIXFAB_INTERFACE" metric 100
    fi
    
    if has_ip_address "wlan0"; then
        local wifi_gw=$(ip route show dev wlan0 | grep -oP 'via \K\S+' | head -1)
        if [ -n "$wifi_gw" ]; then
            sudo ip route add default via "$wifi_gw" dev wlan0 metric 200 2>/dev/null || true
        fi
    fi
}

switch_to_cellular() {
    log "Switching to cellular connection..."
    
    if ! cellular_hardware_present; then
        log "Cannot switch to cellular - hardware not present"
        return 1
    fi
    
    sudo wpa_cli -i wlan0 disconnect
    
    if ! is_cellular_connected; then
        if ! start_cellular; then
            log "Failed to start cellular connection"
            return 1
        fi
    fi
    
    set_cellular_priority
    
    sleep 5
    
    if check_connectivity; then
        log "Successfully switched to cellular (ECM)"
        ensure_btberry_running
        return 0
    else
        log "Failed to establish cellular connection"
        return 1
    fi
}

switch_to_wifi() {
    log "Switching to WiFi connection..."
    
    sudo ip link set wlan0 up
    sudo wpa_cli -i wlan0 reconnect
    sudo systemctl restart wpa_supplicant
    
    sleep 15
    
    if check_connectivity; then
        log "WiFi connected - keeping cellular as backup"
        set_wifi_priority
        ensure_btberry_running
        return 0
    else
        log "Failed to establish WiFi connection"
        return 1
    fi
}

log "=========================================="
log "Network monitor started (Priority-based system with ECM)"
log "WiFi and Cellular stay active (priority switches)"
log "btberry-wifi (Bluetooth) always available"
log "=========================================="

if cellular_hardware_present; then
    log "Cellular hardware detected: $SIXFAB_INTERFACE"
else
    log "No cellular hardware detected - running WiFi-only mode"
fi

ensure_btberry_running
ensure_wifi_running
ensure_cellular_running

sleep 10

PREVIOUS_PRIMARY="none"

while true; do
    ensure_btberry_running
    ensure_wifi_running
    
    if cellular_hardware_present; then
        ensure_cellular_running
    fi
    
    wifi_quality=$(check_wifi_quality)
    current_primary=$(get_primary_interface)
    
    if [ "$wifi_quality" -ge "$WIFI_QUALITY_THRESHOLD" ] && has_ip_address "wlan0"; then
        if [ "$current_primary" != "wlan0" ]; then
            log "Good WiFi available (quality: $wifi_quality%), switching to WiFi priority"
            set_wifi_priority
        fi
        
        if ! check_connectivity; then
            log "WiFi route not working"
            
            if cellular_hardware_present; then
                log "Switching to cellular priority"
                set_cellular_priority
            else
                log "No cellular fallback available (hardware not present)"
            fi
        fi
        
    else
        if [ "$current_primary" != "$SIXFAB_INTERFACE" ]; then
            if [ "$wifi_quality" -gt 0 ]; then
                log "WiFi quality poor ($wifi_quality%)"
            else
                log "WiFi unavailable"
            fi
            
            if cellular_hardware_present; then
                log "Switching to cellular priority"
                set_cellular_priority
            else
                log "No cellular fallback available - staying on WiFi"
            fi
        fi
    fi
    
    current_primary=$(get_primary_interface)
    if [ "$current_primary" != "$PREVIOUS_PRIMARY" ]; then
        log "Primary interface: $PREVIOUS_PRIMARY -> $current_primary"
        PREVIOUS_PRIMARY="$current_primary"
    fi
    
    sleep $WIFI_CHECK_INTERVAL
done
