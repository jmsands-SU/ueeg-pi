import RPi.GPIO as GPIO
import time

# --- Configuration ---
LED_PIN = 17      # GPIO pin number (BCM mode) connected to the LED
FREQUENCY = 41    # The desired frequency in Hertz (Hz)

# --- Calculation ---
# A full cycle (ON and OFF) is the period. Period = 1 / Frequency.
# We need to sleep for half the period for ON and half for OFF.
HALF_PERIOD = 1.0 / (FREQUENCY * 2)

# --- Main Program ---
print(f"Flashing LED on GPIO {LED_PIN} at {FREQUENCY} Hz.")
print(f"Calculated sleep time: {HALF_PERIOD:.6f} seconds.")
print("Press CTRL+C to stop.")

try:
    # Set up the GPIO library
    GPIO.setmode(GPIO.BCM)  # Use Broadcom pin-numbering scheme
    GPIO.setup(LED_PIN, GPIO.OUT) # Set pin as an output
    GPIO.output(LED_PIN, GPIO.LOW) # Start with the LED off

    # Loop indefinitely to flash the LED
    while True:
        GPIO.output(LED_PIN, GPIO.HIGH) # Turn LED ON
        time.sleep(HALF_PERIOD)         # Wait
        GPIO.output(LED_PIN, GPIO.LOW)  # Turn LED OFF
        time.sleep(HALF_PERIOD)         # Wait

except KeyboardInterrupt:
    # This block will run when you press CTRL+C
    print("\nStopping program.")

finally:
    # This block will run no matter how the try block exits
    # It's used to clean up the GPIO channels
    print("Cleaning up GPIO.")
    GPIO.cleanup()
