#!/bin/bash
# install_startup.sh - Complete installation script

set -e

echo "=========================================="
echo "Installing SDR Startup System"
echo "=========================================="
echo ""

if [ "$EUID" -eq 0 ]; then
   echo "Please run as regular user (not root/sudo)"
   echo "The script will ask for sudo password when needed"
   exit 1
fi

# Detect username
CURRENT_USER="${USER:-$(whoami)}"
USER_HOME="/home/${CURRENT_USER}"

echo "Installing for user: ${CURRENT_USER}"

echo ""
echo "Step 1: Installing System Dependencies..."
sudo apt update
sudo apt install -y \
    python3-dev python3-venv git cmake libusb-1.0-0-dev pkg-config \
    libffi-dev build-essential libssl-dev wireless-tools \
    python3-numpy python3-scipy python3-cffi \
    bladerf libbladerf-dev

echo "✓ System dependencies installed"

echo ""
echo "Step 2: Building and Installing BladeRF from Source..."
if [ ! -d "${USER_HOME}/bladeRF" ]; then
    cd "${USER_HOME}"
    git clone https://github.com/Nuand/bladeRF.git
fi
cd "${USER_HOME}/bladeRF/host"
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local ../
make -j$(nproc)
sudo make install
echo "✓ BladeRF library built and installed"

# FIX: Configure shared library path
echo "Configuring shared library path for BladeRF..."
sudo tee /etc/ld.so.conf.d/bladerf.conf > /dev/null << 'EOF'
/usr/local/lib
EOF
sudo ldconfig
echo "✓ Shared library cache updated"

echo ""
echo "Step 3: Installing BladeRF Python Bindings..."
cd "${USER_HOME}/bladeRF/host/libraries/libbladeRF_bindings/python"
echo "Installing Python bindings via direct copy..."
# This direct copy method is more reliable than setup.py or pip on some systems
sudo cp -r bladerf /usr/lib/python3/dist-packages/
echo "✓ Python bindings installed"

# Verify
if python3 -c "import bladerf" 2>/dev/null; then
    echo "✓ BladeRF Python module accessible"
else
    echo "✗ ERROR: BladeRF Python module still not found!"
    exit 1
fi

echo ""
echo "Step 4: Setting up Python Virtual Environment..."
VENV_PATH="${USER_HOME}/sdr_venv"
if [ ! -d "${VENV_PATH}" ]; then
    python3 -m venv --system-site-packages "${VENV_PATH}"
fi
"${VENV_PATH}/bin/pip" install --upgrade pip setuptools wheel
"${VENV_PATH}/bin/pip" install google-cloud-storage google-cloud-pubsub
echo "✓ Python environment configured"

# All other steps remain the same...

echo ""
echo "Step 5: Configuring Headless Boot..."
# ... (rest of install script is the same) ...
sudo cp /boot/config.txt /boot/config.txt.backup.$(date +%Y%m%d) 2>/dev/null || true
if ! grep -q "hdmi_force_hotplug=1" /boot/config.txt; then
    sudo bash -c 'cat >> /boot/config.txt << EOF

# Headless boot configuration
hdmi_force_hotplug=1
hdmi_ignore_edid=0xa5000080
EOF'
fi
echo "✓ Headless boot configured"

echo ""
echo "Step 6: Installing Monitor Scripts..."
sudo cp sdr_monitor.sh /usr/local/bin/
sudo cp network_monitor.sh /usr/local/bin/
sudo chmod +x /usr/local/bin/sdr_monitor.sh
sudo chmod +x /usr/local/bin/network_monitor.sh
echo "✓ Monitor scripts installed"

echo ""
echo "Step 7: Installing Systemd Services..."
sudo cp sdr-monitor.service /etc/systemd/system/
sudo cp network-monitor.service /etc/systemd/system/
echo "✓ Service files installed"


echo ""
echo "Step 8: Creating Log Files..."
sudo mkdir -p /var/log
sudo touch /var/log/sdr_monitor.log /var/log/sdr_service.log /var/log/sdr_python.log /var/log/network_monitor.log
sudo chown "${CURRENT_USER}:${CURRENT_USER}" /var/log/*.log
echo "✓ Log files created"


echo ""
echo "Step 9: Setting up udev rules and user groups..."
sudo tee /etc/udev/rules.d/88-nuand-bladerf.rules > /dev/null << 'EOF'
SUBSYSTEM=="usb", ATTR{idVendor}=="2cf0", ATTR{idProduct}=="5246", MODE="0666", GROUP="plugdev", ATTR{power/autosuspend}="-1"
SUBSYSTEM=="usb", ATTR{idVendor}=="1d50", ATTR{idProduct}=="6066", MODE="0666", GROUP="plugdev", ATTR{power/autosuspend}="-1"
EOF
sudo usermod -a -G plugdev "${CURRENT_USER}"
sudo udevadm control --reload-rules
sudo udevadm trigger
echo "✓ udev rules configured"


echo ""
echo "Step 10: Enabling and Starting Services..."
sudo systemctl daemon-reload
sudo systemctl enable network-monitor.service sdr-monitor.service
sudo systemctl restart network-monitor.service
sleep 5
sudo systemctl restart sdr-monitor.service
echo "✓ Services enabled and started"

echo ""
echo "=========================================="
echo "Installation Complete!"
echo "=========================================="
echo ""
echo "IMPORTANT: A reboot is recommended to apply all changes."
echo "  sudo reboot"
echo ""
