# Detect username
CURRENT_USER="${USER:-$(whoami)}"
USER_HOME="/home/${CURRENT_USER}"
#if current user is root, exit with error
if [ "${CURRENT_USER}" = "root" ]; then
    echo "Please run as regular user (not root/sudo)"
    echo "The script will ask for sudo password when needed"
    exit 1
fi

echo "Installing for user: ${CURRENT_USER}"

# echo ""
echo "Step 1: Installing System Dependencies..."
sudo apt update
sudo apt install -y libbladerf2 cmake libusb-1.0-0-dev gnuradio gr-osmosdr python3-bladerf

# sudo apt install -y \
#     python3-dev python3-venv git cmake libusb-1.0-0-dev pkg-config \  
#     python3-scipy python3-matplotlib \
#     bladerf libbladerf-dev python3-bladerf

echo "✓ System dependencies installed"

echo ""
echo "Step 2: Setting up Python Virtual Environment..."
VENV_PATH="${USER_HOME}/sdr_venv"
if [ ! -d "${VENV_PATH}" ]; then
    python3 -m venv --system-site-packages "${VENV_PATH}"
fi
"${VENV_PATH}/bin/pip" install --upgrade pip setuptools wheel
"${VENV_PATH}/bin/pip" install google-cloud-storage google-cloud-pubsub matplotlib scipy numpy
echo "✓ Python environment configured"

echo ""
echo "Step 7: Setting up udev rules and user groups..."
sudo tee /etc/udev/rules.d/88-nuand-bladerf.rules > /dev/null << 'EOF'
SUBSYSTEM=="usb", ATTR{idVendor}=="2cf0", ATTR{idProduct}=="5246", MODE="0666", GROUP="plugdev", ATTR{power/autosuspend}="-1"
SUBSYSTEM=="usb", ATTR{idVendor}=="1d50", ATTR{idProduct}=="6066", MODE="0666", GROUP="plugdev", ATTR{power/autosuspend}="-1"
EOF
sudo usermod -a -G plugdev "${CURRENT_USER}"
sudo udevadm control --reload-rules
sudo udevadm trigger
echo "✓ udev rules configured"