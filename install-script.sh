#!/bin/bash
# install_startup.sh - Complete installation script

set -e

echo "=========================================="
echo "Installing SDR Startup System"
echo "Priority-Based Network (WiFi/Cellular)"
echo "=========================================="
echo ""

if [ "$EUID" -eq 0 ]; then
   echo "Please run as regular user (not root/sudo)"
   echo "The script will ask for sudo password when needed"
   exit 1
fi

# Detect username
CURRENT_USER=$(whoami)
echo "Installing for user: $CURRENT_USER"

echo ""
echo "Step 1: Installing system dependencies..."

sudo apt update
sudo apt install -y python3-dev libffi-dev build-essential libssl-dev wireless-tools bladerf libbladerf-dev

echo "✓ System dependencies installed"

echo ""
echo "Step 2: Installing Python packages..."

pip3 install --break-system-packages --upgrade pip
pip3 install --break-system-packages google-cloud-storage google-cloud-pubsub scipy numpy matplotlib

echo "✓ Python packages installed"

echo ""
echo "Step 3: Configuring headless boot (no monitor required)..."

sudo cp /boot/config.txt /boot/config.txt.backup.$(date +%Y%m%d) 2>/dev/null || true

if ! grep -q "hdmi_force_hotplug=1" /boot/config.txt; then
    echo "Adding headless boot configuration..."
    sudo bash -c 'cat >> /boot/config.txt << EOF

# Headless boot configuration (added by SDR install script)
hdmi_force_hotplug=1
hdmi_ignore_edid=0xa5000080
EOF'
    echo "✓ Headless boot configured"
else
    echo "✓ Headless boot already configured"
fi

echo ""
echo "Step 4: Installing scripts..."

sudo cp sdr_monitor.sh /usr/local/bin/
sudo cp network_monitor.sh /usr/local/bin/
sudo chmod +x /usr/local/bin/sdr_monitor.sh
sudo chmod +x /usr/local/bin/network_monitor.sh

echo "✓ Scripts installed"

echo ""
echo "Step 5: Installing systemd services..."

sudo cp sdr-monitor.service /etc/systemd/system/
sudo cp network-monitor.service /etc/systemd/system/

echo "✓ Service files installed"

echo ""
echo "Step 6: Creating log files..."

sudo mkdir -p /var/log
sudo touch /var/log/sdr_monitor.log
sudo touch /var/log/sdr_service.log
sudo touch /var/log/sdr_python.log
sudo touch /var/log/network_monitor.log
sudo chown $$CURRENT_USER:$$CURRENT_USER /var/log/sdr_monitor.log
sudo chown $$CURRENT_USER:$$CURRENT_USER /var/log/sdr_service.log
sudo chown $$CURRENT_USER:$$CURRENT_USER /var/log/sdr_python.log
sudo chown $$CURRENT_USER:$$CURRENT_USER /var/log/network_monitor.log

echo "✓ Log files created"

echo ""
echo "Step 7: Configuring btberry-wifi..."

if systemctl list-unit-files | grep -q "btberry-wifi"; then
    sudo systemctl disable btberry-wifi 2>/dev/null || true
    echo "✓ btberry-wifi auto-start disabled (network monitor will manage it)"
else
    echo "⚠ btberry-wifi service not found - install it if needed"
fi

echo ""
echo "Step 8: Verifying Sixfab ECM interface..."

if ip link show usb0 &>/dev/null; then
    echo "✓ Sixfab ECM interface (usb0)
