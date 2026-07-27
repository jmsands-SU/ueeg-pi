#!/usr/bin/env python3
"""
generate_review_slices.py

Scans a directory of per-snapshot .mat files (from
run_snapshot_survey_model_comparison.m), flags the ones that look
interesting (low sync coverage, low reception, high BER, lots of
anomalies), and for each flagged one plots the RAW I/Q power waveform for
that window - so a human can eyeball what actually happened (real RF
dropout vs. multipath vs. interference vs. something else) without
loading MATLAB/Simulink for every single case. Writes a manifest CSV with
blank label/notes columns to fill in.

This is the same "confirmed cases, not just the code's own opinion"
discipline as test_classify_anomalies.py (see that file and
freeze_fixtures.py) - the labels YOU write into the manifest are what
should eventually seed new CONFIRMED_CASES entries there, not this
script's own automatic flags (those are just candidates worth a look).

Usage:
    python generate_review_slices.py <snapshot_dir> <model_label> \
        --file1 <raw ant1 .bin> --file2 <raw ant2 .bin> \
        --out <output dir>

    # example, matching agcslow_slowwalk's raw files:
    python generate_review_slices.py \
        "C:\\Temp\\snapshot_model_comparison\\agcslow_slowwalk" switchtosoftdecode \
        --file1 "\\\\wsl.localhost\\Ubuntu\\home\\joannas\\agcslow_antennashortways_slowwalk.bin" \
        --file2 "\\\\wsl.localhost\\Ubuntu\\home\\joannas\\agcslow_antennashortways_slowwalk_ant2.bin" \
        --out "C:\\Temp\\review_agcslow_slowwalk_switchtosoftdecode"

After running: open --out, flip through the PNGs, fill in the "label" and
"notes" columns in manifest.csv for whichever ones you looked at (leave
the rest blank - only labeled rows are "confirmed", same convention as
CONFIRMED_CASES). Hand the filled-in CSV back for it to be folded into
the confirmed-case registry.
"""
import argparse
import csv
import glob
import os
import sys

import numpy as np
import matplotlib
matplotlib.use("Agg")  # no display needed, just writing PNG files
import matplotlib.pyplot as plt  # noqa: E402

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from compare_snapshot_reception import summarize_snapshot  # noqa: E402
from read_prbs_binary_ch23 import DEFAULT_PRBS_34  # noqa: E402

FS_IQ = 8e6
BYTES_PER_IQ_SAMPLE = 8  # 2 x float32 (I + Q)
POWER_DECIMATION = 100  # boxcar-summed points per plotted point (~4000 pts
                          # for a 0.5s window - decimation now applies AFTER
                          # the 10-sample boxcar sum, not to raw samples, so
                          # this is 10x smaller than before boxcar-summing
                          # was added, to keep the same visual resolution)

# Default flagging thresholds - a snapshot is flagged if ANY of these hold.
# Deliberately permissive defaults (better to over-flag and let a human
# skip the boring ones than miss something) - tune with the CLI flags.
DEFAULT_MAX_COVERAGE = 0.90     # flag if sync coverage < 90%
DEFAULT_MAX_RECEPTION = 0.90    # flag if true reception rate < 90%
DEFAULT_MIN_BIT_BER = 0.03      # flag if bit BER > 3%
DEFAULT_MIN_ANOMALIES = 5       # flag if packet_sequence_anomaly_count >= 5


BOXCAR_LEN = 10  # matches the OOK pulse width used throughout this project's
                  # receiver blocks (rough_snr_detection_50p_hdl.m's own
                  # 10-sample matched-filter boxcar) - raw per-sample power at
                  # 8MHz is dominated by noise between samples, so plotting it
                  # unfiltered just looks like noise even over a real, strong
                  # OOK pulse pattern (confirmed 2026-07-25 - the first version
                  # of this plot showed undifferentiated noise for an entire
                  # window regardless of actual reception quality). Boxcar-sum
                  # first, THEN decimate, so the plot approximates what the
                  # receiver itself sees, not raw sample-level noise.


def read_iq_power(bin_path, start_time, duration, decimation=POWER_DECIMATION, boxcar_len=BOXCAR_LEN):
    """Read a raw I/Q slice and return a decimated, boxcar-matched-filtered
    power trace (max per block, after boxcar-summing to match the OOK pulse
    width) plus the matching time axis. Mirrors the seek/read convention
    used throughout this project (load_snapshot_to_workspace.m,
    run_sim_stream.m)."""
    start_byte = round(start_time * FS_IQ) * BYTES_PER_IQ_SAMPLE
    n_samples = round(duration * FS_IQ)
    with open(bin_path, "rb") as f:
        f.seek(start_byte)
        raw = np.fromfile(f, dtype=np.float32, count=n_samples * 2)
    if len(raw) < 2:
        return np.array([]), np.array([])
    n = len(raw) // 2
    raw = raw[: n * 2]
    i = raw[0::2]
    q = raw[1::2]
    power = i * i + q * q

    # boxcar sum (matched filter for the ~boxcar_len-sample-wide OOK pulse) -
    # cumulative-sum trick for a fast non-overlapping block sum
    n_boxcars = len(power) // boxcar_len
    boxcar_power = power[: n_boxcars * boxcar_len].reshape(n_boxcars, boxcar_len).sum(axis=1)

    n_blocks = max(1, n_boxcars // decimation)
    trimmed = boxcar_power[: n_blocks * decimation]
    power_blocks = trimmed.reshape(n_blocks, decimation).max(axis=1)
    t = start_time + (np.arange(n_blocks) * decimation * boxcar_len + decimation * boxcar_len / 2) / FS_IQ
    return t, power_blocks


def find_snapshots(snapshot_dir, label):
    pattern = os.path.join(snapshot_dir, f"{label}_s*.mat")
    return sorted(glob.glob(pattern))


def should_flag(result, max_coverage, max_reception, min_bit_ber, min_anomalies):
    reasons = []
    if result["coverage"] < max_coverage:
        reasons.append(f"coverage={100*result['coverage']:.1f}%")
    if result["reception_rate"] < max_reception:
        reasons.append(f"reception={100*result['reception_rate']:.1f}%")
    if result.get("ber_prbs", 0) > min_bit_ber:
        reasons.append(f"bitBER={result['ber_prbs']:.4f}")
    if result["anomalies"] >= min_anomalies:
        reasons.append(f"anomalies={result['anomalies']}")
    return reasons


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("snapshot_dir")
    parser.add_argument("label", help="Model label to review, e.g. switchtosoftdecode (must match <label>_s##.mat)")
    parser.add_argument("--file1", required=True, help="Raw ant1 .bin file this recording came from")
    parser.add_argument("--file2", required=True, help="Raw ant2 .bin file this recording came from")
    parser.add_argument("--out", required=True, help="Output directory for PNGs + manifest.csv")
    parser.add_argument("--duration", type=float, default=0.5, help="Snapshot duration in seconds (default 0.5)")
    parser.add_argument("--max-coverage", type=float, default=DEFAULT_MAX_COVERAGE)
    parser.add_argument("--max-reception", type=float, default=DEFAULT_MAX_RECEPTION)
    parser.add_argument("--min-bit-ber", type=float, default=DEFAULT_MIN_BIT_BER)
    parser.add_argument("--min-anomalies", type=int, default=DEFAULT_MIN_ANOMALIES)
    parser.add_argument("--all", action="store_true", help="Plot every snapshot, not just flagged ones")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    pattern = DEFAULT_PRBS_34.copy()

    files = find_snapshots(args.snapshot_dir, args.label)
    if not files:
        print(f"No snapshots found matching {args.label}_s##.mat in {args.snapshot_dir}", file=sys.stderr)
        sys.exit(1)

    manifest_rows = []
    n_flagged = 0

    for f in files:
        snap_idx = int(os.path.basename(f).split("_s")[-1].split(".")[0])
        result = summarize_snapshot(f, prbs=True, pattern=pattern)
        reasons = should_flag(result, args.max_coverage, args.max_reception,
                               args.min_bit_ber, args.min_anomalies)
        flagged = bool(reasons)
        if not (flagged or args.all):
            continue
        n_flagged += 1 if flagged else 0

        # start_time comes back from decode() inside summarize_snapshot via
        # the .mat's own START_TIME field - reload it directly here rather
        # than plumbing it through summarize_snapshot's return value (keeps
        # that function's signature untouched).
        from make_figures import _load_mat
        start_time = float(np.asarray(_load_mat(f, ["START_TIME"])["START_TIME"]).ravel()[0])

        t1, p1 = read_iq_power(args.file1, start_time, args.duration)
        t2, p2 = read_iq_power(args.file2, start_time, args.duration)

        fig, axes = plt.subplots(2, 1, figsize=(11, 5), sharex=True)
        axes[0].plot(t1, p1, linewidth=0.6, color="#33707a")
        axes[0].set_ylabel("ant1 power")
        axes[1].plot(t2, p2, linewidth=0.6, color="#ae4b3b")
        axes[1].set_ylabel("ant2 power")
        axes[1].set_xlabel(f"time in {os.path.basename(args.file1)} (s)")
        flag_str = ", ".join(reasons) if reasons else "not flagged (--all)"
        fig.suptitle(
            f"{args.label} snap {snap_idx:02d}  t={start_time:.3f}-{start_time+args.duration:.3f}s\n"
            f"N={result['N']} cov={100*result['coverage']:.1f}% recv={100*result['reception_rate']:.1f}% "
            f"bitBER={result.get('ber_prbs', float('nan')):.4f} anom={result['anomalies']}\n"
            f"flagged: {flag_str}",
            fontsize=9,
        )
        fig.tight_layout()
        png_name = f"{args.label}_s{snap_idx:02d}.png"
        png_path = os.path.join(args.out, png_name)
        fig.savefig(png_path, dpi=110)
        plt.close(fig)

        manifest_rows.append(dict(
            snapshot_file=os.path.basename(f),
            snap_idx=snap_idx,
            start_time=f"{start_time:.4f}",
            coverage=f"{100*result['coverage']:.1f}",
            reception_rate=f"{100*result['reception_rate']:.1f}",
            bit_ber=f"{result.get('ber_prbs', float('nan')):.4f}",
            anomalies=result["anomalies"],
            auto_flag_reasons=flag_str,
            plot_file=png_name,
            label="",      # fill in: what actually happened here
            notes="",      # fill in: anything worth keeping in mind
        ))
        print(f"  wrote {png_name}  ({flag_str})")

    manifest_path = os.path.join(args.out, "manifest.csv")
    with open(manifest_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(manifest_rows[0].keys()) if manifest_rows else
                                 ["snapshot_file", "snap_idx", "start_time", "coverage", "reception_rate",
                                  "bit_ber", "anomalies", "auto_flag_reasons", "plot_file", "label", "notes"])
        writer.writeheader()
        writer.writerows(manifest_rows)

    print(f"\n{n_flagged} flagged / {len(files)} total snapshots for {args.label}.")
    print(f"Plots + manifest.csv written to {args.out}")
    print(f"Fill in the 'label'/'notes' columns for whichever rows you review, then hand the CSV back.")


if __name__ == "__main__":
    main()
