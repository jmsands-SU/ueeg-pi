#!/usr/bin/env python3
"""
gpio_pulse_timer.py — output random GPIO pulses and log their timestamps.

Fires a 10 ms pulse on the output pin at random intervals (uniform between
--min-interval and --max-interval seconds). Output JSON is identical to
tap_timer.py so the same latency analysis applies.

Usage:
    sudo pigpiod          # start daemon if not running
    python3 gpio_pulse_timer.py --pin 17
    python3 gpio_pulse_timer.py --pin 17 --out pulses.json --min-interval 1 --max-interval 5

Output JSON fields per pulse:
    tap_idx        — sequential pulse number (0-based)
    system_time_s  — wall-clock time.time() at pulse onset (matches timestamp_log field)
    timestamp_utc  — ISO UTC string for human readability
    dt_s           — seconds since previous pulse (null for first)
"""

import argparse
import json
import random
import signal
import sys
import time

try:
    import pigpio
except ImportError:
    sys.exit("pigpio not found. Install with: pip install pigpio  (and run: sudo pigpiod)")

PULSE_WIDTH_S = 0.010  # 10 ms pulse


def run(pin: int, out_path: str, min_interval: float, max_interval: float):
    pig = pigpio.pi()
    if not pig.connected:
        sys.exit("Could not connect to pigpio daemon. Is it running? (sudo pigpiod)")

    pig.set_mode(pin, pigpio.OUTPUT)
    pig.write(pin, 0)

    pulses: list[dict] = []

    def _shutdown(sig=None, frame=None):
        pig.write(pin, 0)
        pig.stop()
        print(f"\nSaving {len(pulses)} pulses to {out_path}")
        with open(out_path, "w") as f:
            json.dump({"taps": pulses}, f, indent=2)
        print("Done.")
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"gpio_pulse_timer.py ready — outputting on GPIO{pin}, "
          f"interval {min_interval}–{max_interval}s.")
    print(f"Ctrl+C to stop and save to {out_path}\n")

    while True:
        interval = random.uniform(min_interval, max_interval)
        time.sleep(interval)

        now = time.time()
        pig.write(pin, 1)
        time.sleep(PULSE_WIDTH_S)
        pig.write(pin, 0)

        utc = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now))
        subsec = f"{now % 1:.3f}"[1:]
        dt_s = round(now - pulses[-1]["system_time_s"], 6) if pulses else None

        pulse = {
            "tap_idx": len(pulses),
            "system_time_s": now,
            "timestamp_utc": utc + subsec + "Z",
            "dt_s": dt_s,
        }
        pulses.append(pulse)

        dt_str = f"  +{dt_s:.3f}s" if dt_s is not None else ""
        print(f"  pulse {pulse['tap_idx']:4d}  system_time_s: {now:.7f}{dt_str}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate random GPIO pulses and log timestamps")
    parser.add_argument("--pin", type=int, default=17, help="BCM GPIO pin number (default: 17)")
    parser.add_argument("--out", default="pulses.json", help="Output JSON file (default: pulses.json)")
    parser.add_argument("--min-interval", type=float, default=1.0,
                        help="Minimum seconds between pulses (default: 1.0)")
    parser.add_argument("--max-interval", type=float, default=3.0,
                        help="Maximum seconds between pulses (default: 3.0)")
    args = parser.parse_args()

    if args.min_interval < 1.0:
        parser.error("--min-interval must be at least 1.0 s")

    run(args.pin, args.out, args.min_interval, args.max_interval)
