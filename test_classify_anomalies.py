#!/usr/bin/env python3
"""
test_classify_anomalies.py

Regression test for compare_prbs_ber.py's --classify anomaly labels
(_classify_anomalies / _find_frame_gaps / _find_chain_slips), backed by a
small registry of REAL cases with independently-confirmed ground truth
(scope screenshots, controlled no-chunking re-runs - not just "the code
says so"). Run this after ANY change to the classification logic - it
exists specifically because the classifier has been wrong before
(USB_DROP based on "both copies missing", REJECTED_FRAME assumed to be RF
corruption instead of a USB drop, the original 15m case wrongly assumed
to be CLEAN_GAP) and each of those mistakes was only caught by manually
re-checking against real data.

STANDALONE BY DESIGN (fixed 2026-07-25): this used to re-decode whatever
happened to currently be sitting in C:\\Temp\\prbs_length_comparison\\*.mat
- scratch space that gets overwritten routinely (we did it a dozen times
this session alone). That meant the test wasn't actually pinned to a
known, fixed case at all - if that scratch file changed or got deleted,
the test would either silently test different data or just break, with
no record of what it was originally supposed to be checking. Now it loads
frozen .npz fixtures from test_fixtures/ (checked in alongside this file)
instead - see freeze_fixtures.py, which is what produces them. Run that
script (once) whenever a new CONFIRMED_CASES entry references a .mat file
that isn't already frozen.

Confirmed cases only go in CONFIRMED_CASES once someone has actually
looked at the raw I/Q on a scope (or equally direct evidence, like a
controlled re-run with the suspected root cause mechanism removed) and
matched it to a label - not from the classifier's own output, which would
make this test circular and useless.

Usage:
    python test_classify_anomalies.py
"""
import os
import sys

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
from sdr_reader_gcs_write import TimeStampBasedReader  # noqa: E402
from read_prbs_binary_ch23 import compute_prbs_ber, DEFAULT_PRBS_34  # noqa: E402
import compare_prbs_ber as cpb  # noqa: E402

FIXTURE_DIR = os.path.join(_HERE, "test_fixtures")

# --- Confirmed reference cases -----------------------------------------
# Each entry: (mat_filename, priority ['v1_only'|'v2_only'], idx,
#              expected_label, how it was confirmed).
# idx is the decoded-sample index (matches the "idx" column --classify
# prints) - NOT a raw word position. mat_filename identifies which frozen
# fixture in test_fixtures/ to use (see freeze_fixtures.py) - it does NOT
# read from C:\Temp\prbs_length_comparison at all anymore.
CONFIRMED_CASES = [
    (
        "15meters_switchtosoftdecode_ant2.mat", "v1_only", 503, "REJECTED_FRAME",
        "scope screenshot 2026-07-25, t~2.52s - a 602-word invalid-length "
        "frame sits inside this gap (raw log: 'Frame rejected (len=602...) "
        "@ word=251527 t=2.515s') - originally miscategorized as CLEAN_GAP "
        "before this was checked",
    ),
    (
        "15meters_switchtosoftdecode_ant2.mat", "v2_only", 503, "REJECTED_FRAME",
        "same event as above, seen from the v2_only pass",
    ),
    (
        "10meters_switchtosoftdecode_ant2.mat", "v1_only", 1395, "REJECTED_FRAME",
        "scope screenshot 2026-07-25, t~6.97s (frame_len=408 vs normal ~250, "
        "is_valid=False)",
    ),
    (
        "10meters_switchtosoftdecode_ant2.mat", "v2_only", 1598, "CHUNK_EDGE",
        "confirmed 2026-07-25 by a controlled test, not just a scope look: "
        "loaded the same raw window (t~7.9-8.2s) through "
        "load_snapshot_to_workspace.m as ONE continuous simulation (no "
        "run_sim_stream.m chunking at all) and the anomaly did not "
        "reproduce - only startup-transient behavior at the very start of "
        "the loaded window, nothing at this timestamp",
    ),
    (
        "20meters_throughwall_switchtosoftdecode_ant2.mat", "v2_only", 398, "CHAIN_SLIP",
        "confirmed 2026-07-25 via the same controlled no-chunking test "
        "(t~1.9-2.2s window) - anomaly did not reproduce. This candidate "
        "sits almost exactly on the t=2.0s chunk boundary of the original "
        "run_sim_stream.m call, consistent with the chunk-restart theory",
    ),
    (
        "20meters_throughwall_switchtosoftdecode_ant2.mat", "v2_only", 1377, "CHAIN_SLIP",
        "confirmed 2026-07-25 via the same controlled no-chunking test "
        "(t~6.8-7.1s window) - anomaly did not reproduce, even though this "
        "candidate is NOT close to an obvious chunk boundary (nearest is "
        "t=6.0s or 8.0s, off by ~0.9s) - so chunk-restart artifacts aren't "
        "strictly confined to the exact boundary instant",
    ),
]

# --- Negative/sanity cases: a label that must NOT be there --------------
# Cheap protection against a change that makes the classifier over-eager
# (e.g. flagging everything as one label).
FORBIDDEN_LABELS = [
    ("15meters_switchtosoftdecode_ant2.mat", "v1_only", 503, "CLEAN_GAP"),
]


def _classify_from_fixture(mat_filename):
    fixture_path = os.path.join(FIXTURE_DIR, mat_filename.replace(".mat", ".npz"))
    if not os.path.exists(fixture_path):
        raise FileNotFoundError(
            f"No frozen fixture for {mat_filename} at {fixture_path} - "
            f"run `python freeze_fixtures.py {mat_filename}` first"
        )
    data = np.load(fixture_path)
    v1_bits = data["v1_bits"]
    v2_bits = data["v2_bits"]
    v1_missing = data["v1_missing"]
    v2_missing = data["v2_missing"]
    v1_word_pos = data["v1_word_pos"]
    v2_word_pos = data["v2_word_pos"]
    start_time = float(data["start_time"][0])
    N = len(v1_bits)

    # Rebuild a minimal stand-in for the frame log a real TimeStampBasedReader
    # would carry - NOT a re-decode, this is exactly what was frozen. Reuses
    # the real class only for its stateless _estimate_frames_in_gap_linear
    # method (which only depends on accepted_frame_lengths, set at
    # construction) - decode_from_word_stream is never called on it, so no
    # actual decoding happens here, just borrowing that one method.
    reader = TimeStampBasedReader(
        enable_gcs=False, quiet=True, gcs_channels=(1, 2, 3, 4),
        accepted_frame_lengths=(250, 248, 251), frame_length_counts={250: 18, 248: 1},
    )
    reader._raw_frame_log = list(zip(
        data["raw_frame_log_abs_words"].tolist(),
        data["raw_frame_log_pkt_nums"].tolist(),
        data["raw_frame_log_frame_lens"].tolist(),
        data["raw_frame_log_is_valids"].tolist(),
    ))

    frame_len_lookup = {int(w): int(fl) for w, pkt_n, fl, is_valid in reader._raw_frame_log}

    def is_chunk_edge(word_pos):
        out = np.zeros(len(word_pos), dtype=bool)
        for i in range(len(word_pos)):
            wp = int(word_pos[i])
            if wp >= 0 and frame_len_lookup.get(wp) == 251:
                out[i] = True
        return out
    chunk_edge = is_chunk_edge(v1_word_pos) | is_chunk_edge(v2_word_pos)

    pattern = DEFAULT_PRBS_34.copy()
    rows1, *_ = compute_prbs_ber(v1_bits, v2_bits, v1_missing, v2_missing, pattern, cpb.BIT_SLICE, copy_priority="v1_only")
    rows2, *_ = compute_prbs_ber(v1_bits, v2_bits, v1_missing, v2_missing, pattern, cpb.BIT_SLICE, copy_priority="v2_only")
    slip1, _ = cpb._find_chain_slips(rows1, cpb.PATTERN_LEN)
    slip2, _ = cpb._find_chain_slips(rows2, cpb.PATTERN_LEN)
    chain_slip = slip1 | slip2

    return {
        "v1_only": cpb._classify_anomalies(rows1, v1_missing, v2_missing, v1_word_pos, v2_word_pos,
                                            start_time, chain_slip, chunk_edge, reader, N),
        "v2_only": cpb._classify_anomalies(rows2, v1_missing, v2_missing, v1_word_pos, v2_word_pos,
                                            start_time, chain_slip, chunk_edge, reader, N),
    }


def main():
    _cache = {}

    def get(mat_filename):
        if mat_filename not in _cache:
            _cache[mat_filename] = _classify_from_fixture(mat_filename)
        return _cache[mat_filename]

    failures = 0

    for mat_filename, priority, idx, expected_label, confirmed_via in CONFIRMED_CASES:
        classified = get(mat_filename)[priority]
        row = next((c for c in classified if c["idx"] == idx), None)
        if row is None:
            print(f"FAIL  {mat_filename} [{priority}] idx={idx}: sample not flagged at all "
                  f"(expected {expected_label}) - confirmed via: {confirmed_via}")
            failures += 1
        elif expected_label not in row["labels"]:
            print(f"FAIL  {mat_filename} [{priority}] idx={idx}: got {row['labels']}, "
                  f"expected {expected_label} - confirmed via: {confirmed_via}")
            failures += 1
        else:
            print(f"PASS  {mat_filename} [{priority}] idx={idx}: {row['labels']}")

    for mat_filename, priority, idx, forbidden_label in FORBIDDEN_LABELS:
        classified = get(mat_filename)[priority]
        row = next((c for c in classified if c["idx"] == idx), None)
        if row is not None and forbidden_label in row["labels"]:
            print(f"FAIL  {mat_filename} [{priority}] idx={idx}: got forbidden label "
                  f"{forbidden_label} in {row['labels']}")
            failures += 1
        else:
            print(f"PASS  {mat_filename} [{priority}] idx={idx}: does not have {forbidden_label}")

    print(f"\n{len(CONFIRMED_CASES) + len(FORBIDDEN_LABELS) - failures}/"
          f"{len(CONFIRMED_CASES) + len(FORBIDDEN_LABELS)} checks passed.")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
