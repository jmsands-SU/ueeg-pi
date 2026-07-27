#!/usr/bin/env python3
"""
freeze_fixtures.py

Freezes the decoded output needed by test_classify_anomalies.py's
CONFIRMED_CASES/FORBIDDEN_LABELS into standalone .npz files under
test_fixtures/, so the regression test doesn't depend on
C:\\Temp\\prbs_length_comparison\\*.mat still existing or being unchanged.
That directory is scratch space that's been overwritten routinely all
session - flagged as a real problem 2026-07-25 (a test that silently
re-decodes whatever happens to currently be sitting in a scratch path
isn't testing a fixed, known case at all).

Run this whenever a NEW confirmed case is added that references a .mat
file not already frozen:

    python freeze_fixtures.py                          # freeze everything referenced but missing
    python freeze_fixtures.py some_file.mat             # freeze one specific file
    python freeze_fixtures.py some_file.mat --force     # re-freeze even if already frozen

Existing fixtures are never silently overwritten without --force.
"""
import argparse
import os
import sys

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from make_figures import _load_mat  # noqa: E402
from sdr_reader_gcs_write import TimeStampBasedReader  # noqa: E402
import compare_prbs_ber as cpb  # noqa: E402

DATA_DIR = r"C:\Temp\prbs_length_comparison"
FIXTURE_DIR = os.path.join(_HERE, "test_fixtures")


def freeze(mat_filename, force=False):
    out_path = os.path.join(FIXTURE_DIR, mat_filename.replace(".mat", ".npz"))
    if os.path.exists(out_path) and not force:
        print(f"SKIP (already frozen, use --force to redo): {out_path}")
        return

    mat_path = os.path.join(DATA_DIR, mat_filename)
    d = _load_mat(mat_path, cpb.VARNAMES + ["START_TIME"])
    start_time = float(np.asarray(d["START_TIME"]).ravel()[0])
    push, databit, error, packetnum = (np.asarray(d[k], dtype=np.uint16).ravel() for k in cpb.VARNAMES)
    push, databit, error, packetnum = cpb._align([push, databit, error, packetnum])
    words = ((databit & 0x1) | ((packetnum & 0x7) << 4) | ((error & 0x1) << 7) | ((push & 0x1) << 8)).astype(np.uint16)

    reader = TimeStampBasedReader(
        enable_gcs=False, quiet=True, gcs_channels=(1, 2, 3, 4),
        accepted_frame_lengths=(250, 248, 251), frame_length_counts={250: 18, 248: 1},
    )
    reader.decode_from_word_stream(words)

    bits_ch2 = reader.get_decoded_bits(2).ravel()
    bits_ch3 = reader.get_decoded_bits(3).ravel()
    v1_bits = np.concatenate([bits_ch2["v1_bits"], bits_ch3["v1_bits"]], axis=1)
    v2_bits = np.concatenate([bits_ch2["v2_bits"], bits_ch3["v2_bits"]], axis=1)
    v1_missing = bits_ch2["v1_missing"] | bits_ch3["v1_missing"]
    v2_missing = bits_ch2["v2_missing"] | bits_ch3["v2_missing"]
    v1_word_pos = bits_ch2["v1_word_pos"]
    v2_word_pos = bits_ch2["v2_word_pos"]

    raw_frame_log = reader._raw_frame_log
    abs_words = np.array([r[0] for r in raw_frame_log], dtype=np.int64)
    pkt_nums = np.array([r[1] for r in raw_frame_log], dtype=np.int64)
    frame_lens = np.array([r[2] for r in raw_frame_log], dtype=np.int64)
    is_valids = np.array([r[3] for r in raw_frame_log], dtype=bool)

    os.makedirs(FIXTURE_DIR, exist_ok=True)
    np.savez_compressed(
        out_path,
        v1_bits=v1_bits, v2_bits=v2_bits,
        v1_missing=v1_missing, v2_missing=v2_missing,
        v1_word_pos=v1_word_pos, v2_word_pos=v2_word_pos,
        start_time=np.array([start_time]),
        raw_frame_log_abs_words=abs_words,
        raw_frame_log_pkt_nums=pkt_nums,
        raw_frame_log_frame_lens=frame_lens,
        raw_frame_log_is_valids=is_valids,
        source_mat_filename=np.array([mat_filename]),
    )
    print(f"Froze {mat_filename} -> {out_path} "
          f"(N={len(v1_bits)} samples, {len(raw_frame_log)} raw frames)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mat_filenames", nargs="*",
                         help="Specific .mat filenames to freeze; defaults to every unique "
                              "file referenced in test_classify_anomalies.py")
    parser.add_argument("--force", action="store_true", help="Re-freeze even if already frozen")
    args = parser.parse_args()

    if args.mat_filenames:
        targets = args.mat_filenames
    else:
        import test_classify_anomalies as tca
        targets = sorted(set(
            [c[0] for c in tca.CONFIRMED_CASES] + [c[0] for c in tca.FORBIDDEN_LABELS]
        ))

    for f in targets:
        freeze(f, force=args.force)
