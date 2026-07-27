#!/usr/bin/env python3
"""
compare_full_recording_reception.py

Reception-rate comparison between two Simulink model runs, each a single
continuous simulation across the full wall-clock-bounded recording window
(from run_full_recording_model_comparison.m) - not sparse snapshots. Real
walking-recording data, no PRBS ground truth, so this reports packet-
tracking/reception stats (same metrics as compare_snapshot_reception.py,
see that file's docstring for why packet_num-only regularity isn't used)
rather than BER, broken down per antenna config (ant1/ant2/both) instead
of per snapshot-in-time.

Usage:
    python compare_full_recording_reception.py <out_dir> --labels A B
        Expects files named "<label>_ant1.mat", "<label>_ant2.mat",
        "<label>_both.mat" in out_dir (run_full_recording_model_
        comparison.m's naming convention).
"""
import argparse
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from compare_snapshot_reception import summarize_snapshot  # noqa: E402

CONFIGS = ["ant1", "ant2", "both"]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("out_dir", help="Directory containing <label>_<config>.mat files")
    parser.add_argument("--labels", nargs=2, required=True, help="Two model labels (must match filename prefixes)")
    args = parser.parse_args()
    label_a, label_b = args.labels

    print(f"\n{'='*100}")
    print(f"FULL-RECORDING RECEPTION COMPARISON  ({label_a}  vs  {label_b})")
    print(f"{'='*100}")

    header = (f"{'config':>6}  {'N':>5}  "
              f"{'recv%':>7} {'recv%':>7}  "
              f"{'v1miss%':>8} {'v1miss%':>8}  "
              f"{'v2miss%':>8} {'v2miss%':>8}  "
              f"{'bothmiss%':>10} {'bothmiss%':>10}  "
              f"{'x-BER':>8} {'x-BER':>8}  "
              f"{'anom':>5} {'anom':>5}  "
              f"{'hdrop':>5} {'hdrop':>5}  "
              f"{'edge':>5} {'edge':>5}")
    print(header)
    print(f"{'':>6}  {'':>5}  {label_a:>7} {label_b:>7}  {label_a:>8} {label_b:>8}  "
          f"{label_a:>8} {label_b:>8}  {label_a:>10} {label_b:>10}  "
          f"{label_a:>8} {label_b:>8}  {label_a:>5} {label_b:>5}  "
          f"{label_a:>5} {label_b:>5}  {label_a:>5} {label_b:>5}")

    for cfg in CONFIGS:
        path_a = os.path.join(args.out_dir, f"{label_a}_{cfg}.mat")
        path_b = os.path.join(args.out_dir, f"{label_b}_{cfg}.mat")
        if not (os.path.exists(path_a) and os.path.exists(path_b)):
            print(f"{cfg:>6}  (missing file(s), skipped)")
            continue
        ra = summarize_snapshot(path_a)
        rb = summarize_snapshot(path_b)
        print(f"{cfg:>6}  {ra['N']:>5}  "
              f"{100*ra['reception_rate']:>7.1f} {100*rb['reception_rate']:>7.1f}  "
              f"{100*ra['v1_missing_rate']:>8.2f} {100*rb['v1_missing_rate']:>8.2f}  "
              f"{100*ra['v2_missing_rate']:>8.2f} {100*rb['v2_missing_rate']:>8.2f}  "
              f"{100*ra['both_missing_rate']:>10.2f} {100*rb['both_missing_rate']:>10.2f}  "
              f"{ra['cross_copy_ber']:>8.4f} {rb['cross_copy_ber']:>8.4f}  "
              f"{ra['anomalies']:>5} {rb['anomalies']:>5}  "
              f"{ra['header_drops']:>5} {rb['header_drops']:>5}  "
              f"{ra['n_chunk_edge']:>5} {rb['n_chunk_edge']:>5}")

    print(f"\nreception% = fraction of decoded sample-slots with at least one copy (v1 or v2) present")
    print(f"x-BER = cross-copy BER (v1 vs v2 direct comparison, no ground truth needed)")
    print(f"'ant1'/'ant2' configs: v1/v2 both come from the SAME antenna (file1→both data & data2, "
          f"see run_three_sims_and_plot.m) - v1/v2 there are the receiver's own duplicate-transmission "
          f"copies, not a second antenna")


if __name__ == "__main__":
    main()
