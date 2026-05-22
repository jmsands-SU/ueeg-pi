#!/usr/bin/env python3
"""
combined_diversity_plot.py

Combines per-window antenna diversity (push1 / changeofstrength1 from Simulink
mat files) with time diversity (quality codes decoded by TimeStampBasedReader)
into two matplotlib figures.

Figure 1 — time-domain view (6 linked subplots, shared x-axis):
  [0]  Antenna 1  |IQ| magnitude (downsampled to per-window resolution)
  [1]  Antenna 2  |IQ| magnitude
  [2]  Ant-1-only  received per window  (step plot, 0=miss / 1=ok)
  [3]  Ant-2-only  received per window
  [4]  Both antennas  received per window  (antenna diversity)
  [5]  Both antennas  time-diversity quality colour strip
         green  = at least one copy received correctly   (q ∈ {3,5,6})
         yellow = exactly one copy received              (q ∈ {1,2})
         red    = neither copy received                  (q == 0)

Figure 2 — reception-rate bar chart (3 antenna configs × 2 time-div scenarios):
  For each of the three antenna configurations (ant1 only, ant2 only, both):
    • Blue bar  – push1-based reception rate (antenna diversity; time diversity
                  already folded in if the Simulink model accepts either copy)
    • Orange bar – no-time-diversity baseline reception rate, computed from
                   quality codes as: 1 – (v1_only + v2_only + 2×neither) / (2N)
                   (symmetric average of v1-primary and v2-primary scenarios)
    • Green bar  – with-time-diversity reception rate = both_recv / N

Usage (WSL):
    python combined_diversity_plot.py [start_time [max_duration]]

Defaults: start_time=8, max_duration=1

Requirements:
    numpy, scipy, matplotlib
    TimeStampBasedReader — place the .py file containing that class in the same
    directory as this script (it will be added to sys.path automatically).
    If not found the script still runs; Figure 1 rows [3-5] and Figure 2
    orange/green bars are simply omitted.
"""

import sys
import os
import numpy as np
import scipy.io
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as mcolors

# ── TimeStampBasedReader import ────────────────────────────────────────────────
_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _DIR)
HAS_TSBR = False
try:
    from sdr_reader_gcs_write import TimeStampBasedReader
    HAS_TSBR = True
except ImportError:
    print('WARNING: TimeStampBasedReader not importable.\n'
          '  sdr_reader_gcs_write.py must be in the same directory as this script.\n'
          '  Time-diversity subplot and bars will be skipped.\n')

# ── File paths ─────────────────────────────────────────────────────────────────
# MATLAB writes mat files to C:\Temp; those appear at /mnt/c/Temp in WSL.
MAT_ANT1 = '/mnt/c/Temp/interferencelosstest_ant1only.mat'
MAT_ANT2 = '/mnt/c/Temp/interferencelosstest_ant2only.mat'
MAT_BOTH = '/mnt/c/Temp/interferencelosstest_both.mat'

# Raw IQ binaries (float32 interleaved I/Q, 8 MHz)
IQ_FILE1 = '/home/joannas/interferencelosstest_ant1.bin'
IQ_FILE2 = '/home/joannas/interferencelosstest_ant2.bin'

# ── Parameters ─────────────────────────────────────────────────────────────────
PACKET_RATE  = 400      # Hz  — packet windows are 2.5 ms wide
FS_IQ        = 8e6     # Hz  — raw IQ binary sample rate
SKIP_S       = 0.05    # s   — ignore first 50 ms to avoid startup misdetections
CONFLICT_THR = 10      # co-occurring push1 + changeofstrength1 samples ≤ this = valid
GCS_CHANNEL  = '2'      # channel index for TimeStampBasedReader.get_decoded_arrays()
FRAME_LEVEL_STEP_PLOTS = False  # True → step plots at raw frame rate (~400 Hz, pre-diversity)
                                 # False → quality-code based (~200 Hz EEG rate)


# ══════════════════════════════════════════════════════════════════════════════
# _load_mat  — handles MATLAB v5 (scipy.io) and v7.3/HDF5 (h5py)
# ══════════════════════════════════════════════════════════════════════════════
def _load_mat(filepath, varnames):
    """
    Load named variables from a MATLAB .mat file.
    Transparently handles v5 (scipy.io) and v7.3 HDF5 (h5py).

    Returns a dict {name: value}.
    1-D / column-vector arrays → float64 ndarray, shape (N,).
    Scalars → Python float.
    """
    # ── scipy.io path (v4 / v5 / v6) ─────────────────────────────────────────
    try:
        d = scipy.io.loadmat(filepath, squeeze_me=True,
                             variable_names=list(varnames))
        return {k: d[k] for k in varnames if k in d}
    except NotImplementedError:
        pass          # v7.3 HDF5 — fall through to h5py
    except Exception as exc:
        raise RuntimeError(f'scipy.io.loadmat failed: {exc}') from exc

    # ── h5py path (v7.3) ──────────────────────────────────────────────────────
    try:
        import h5py
    except ImportError as exc:
        raise RuntimeError(
            'Mat file is MATLAB v7.3 (HDF5) but h5py is not installed.\n'
            'Install it with:  pip install h5py') from exc

    result = {}
    with h5py.File(filepath, 'r') as f:
        for k in varnames:
            if k not in f:
                continue
            raw = f[k][()]                          # read dataset as numpy array
            if raw.ndim == 0 or raw.size == 1:
                result[k] = float(raw.ravel()[0])   # scalar
            else:
                # MATLAB writes column-major; HDF5 row-major read transposes it.
                # ravel() collapses (N,1) or (1,N) to (N,) either way.
                result[k] = raw.ravel().astype(np.float64)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# read_iq_magnitude
# ══════════════════════════════════════════════════════════════════════════════
def read_iq_magnitude(filepath, start_time, duration):
    """Return |IQ| magnitude vector from a float32-interleaved binary file."""
    n_samp       = round(duration * FS_IQ)
    offset_bytes = round(start_time * FS_IQ) * 8   # 2 × float32 = 8 bytes/sample
    with open(filepath, 'rb') as fh:
        fh.seek(offset_bytes)
        raw = np.frombuffer(fh.read(n_samp * 8), dtype=np.float32).copy()
    if len(raw) % 2:
        raw = np.append(raw, np.float32(0))
    return np.abs(raw[0::2] + 1j * raw[1::2])


# ══════════════════════════════════════════════════════════════════════════════
# compute_frame_reception  — pre-diversity frame-level ok/miss at ~400 Hz
# ══════════════════════════════════════════════════════════════════════════════
def compute_frame_reception(words, start_time, fs_words=100_000, frame_size=250):
    """
    Score each frame slot as received/missed using sequence-number validation.
    v1 and v2 are treated as independent packets at their actual arrival times.
    No v1/v2 combining — this is the pre-diversity, pre-combining view.

    Returns:
        received  bool (N_frames,)
        t_frames  float (N_frames,)  absolute time of frame centre (s)
    """
    push_arr   = ((words >> 8) & 1).astype(np.uint8)
    error_arr  = ((words >> 7) & 1).astype(np.uint8)
    pktnum_arr = ((words >> 4) & 0x7).astype(np.uint8)

    N_frames = len(words) // frame_size
    received = np.zeros(N_frames, dtype=bool)
    dt       = frame_size / fs_words
    t_frames = start_time + (np.arange(N_frames) + 0.5) * dt

    expected = -1
    for f in range(N_frames):
        sl    = slice(f * frame_size, (f + 1) * frame_size)
        valid = push_arr[sl] == 1
        if not valid.any():
            continue
        pkt      = int(np.median(pktnum_arr[sl][valid]))
        has_error = bool(np.any(error_arr[sl][valid]))
        in_seq    = (expected < 0 or pkt == expected)
        if in_seq and not has_error:
            received[f] = True
        expected = (pkt + 1) % 8   # resync on any arrival

    return received, t_frames


# ══════════════════════════════════════════════════════════════════════════════
# compute_reception  — replicates run_three_sims_and_plot.m logic
# ══════════════════════════════════════════════════════════════════════════════
def compute_reception(mat_path, max_duration):
    """
    Load a Simulink mat file and compute per-window packet reception.

    Window validity rules (matching MATLAB):
      1. push1 duty cycle > 25 % (push1 has a 50 % duty cycle when a packet is
         being received, so 25 % is the minimum for a clean half-window hit).
      2. No more than CONFLICT_THR samples have push1 == 1 AND
         changeofstrength1 == 1 simultaneously.

    Returns dict:
        received    bool  (nWindows,)
        t_windows   float (nWindows,)  centre time of each window in seconds
        rate        float              fraction of windows received (0–1)
        Fs_push     float              inferred push1 sample rate (Hz)
        WINDOW      int                samples per packet window at Fs_push
        first_one   int                push1 index where aligned windows start
        START_TIME  float
        push1       float (N,)
    Returns None if the file cannot be processed.
    """
    try:
        d = _load_mat(mat_path, ['push1', 'changeofstrength1', 'Fs', 'START_TIME'])
    except Exception as exc:
        print(f'  Cannot load {mat_path}: {exc}')
        return None

    if 'push1' not in d:
        print(f'  push1 not found in {mat_path}')
        return None

    push1      = np.asarray(d['push1'],           dtype=float).ravel()
    START_TIME = float(d.get('START_TIME', 0.0))
    has_cos    = 'changeofstrength1' in d
    cos1       = (np.asarray(d['changeofstrength1'], dtype=float).ravel()
                  if has_cos else None)

    # Infer push1 sample rate from recording duration
    Fs_push = len(push1) / max_duration
    WINDOW  = round(Fs_push / PACKET_RATE)
    print(f'  Fs_push = {Fs_push:.0f} Hz  →  WINDOW = {WINDOW} samples '
          f'({1000 * WINDOW / Fs_push:.2f} ms)')

    # ── Find first valid rising edge after SKIP_S startup ─────────────────────
    skip    = round(SKIP_S * Fs_push)
    trimmed = push1[skip:]
    edges   = np.where(np.diff(np.concatenate([[0.0], trimmed])) == 1)[0]

    first_one = None
    for k in edges:
        j = k - 1
        run_zeros = 0
        while j >= 0 and trimmed[j] == 0:
            run_zeros += 1
            j -= 1
        if run_zeros > 5:
            first_one = skip + k
            break

    if first_one is None:
        print(f'  WARNING: no valid push1 onset found — skipping {mat_path}')
        return None

    # ── Reshape into (nWindows, WINDOW) blocks ─────────────────────────────────
    # numpy C-order reshape(nWindows, WINDOW) produces the same grouping as
    # MATLAB's column-major reshape(WINDOW, nWindows).
    aligned  = push1[first_one:]
    nWindows = len(aligned) // WINDOW
    if nWindows == 0:
        print(f'  WARNING: no complete windows after onset — skipping {mat_path}')
        return None
    blocks = aligned[:nWindows * WINDOW].reshape(nWindows, WINDOW)

    push1_duty = blocks.mean(axis=1)   # fraction of 1s per window

    if has_cos:
        cos_aligned  = cos1[first_one:]
        cos_blocks   = cos_aligned[:nWindows * WINDOW].reshape(nWindows, WINDOW)
        conflict_ct  = (blocks.astype(bool) & cos_blocks.astype(bool)).sum(axis=1)
        no_conflict  = conflict_ct <= CONFLICT_THR
        n_bad        = int((~no_conflict).sum())
        if n_bad:
            print(f'  {n_bad} window(s) had push1 & changeofstrength1 '
                  f'simultaneously high (flagged as invalid)')
        received = (push1_duty > 0.25) & no_conflict
    else:
        received = push1_duty > 0.25

    win_starts = first_one + np.arange(nWindows) * WINDOW
    t_windows  = START_TIME + (win_starts + WINDOW / 2.0) / Fs_push

    rate = float(received.mean())
    if rate < 0.5:
        print(f'  WARNING: only {100*rate:.1f}% of windows received')

    return dict(received=received, t_windows=t_windows, rate=rate,
                Fs_push=Fs_push, WINDOW=WINDOW, first_one=first_one,
                START_TIME=START_TIME, push1=push1)


# ══════════════════════════════════════════════════════════════════════════════
# decode_time_diversity  — uses TimeStampBasedReader
# ══════════════════════════════════════════════════════════════════════════════
def decode_time_diversity(mat_path, max_duration):
    """
    Decode time-diversity quality codes from a Simulink mat file.

    Quality nibble (base code = q & 0x07):
        0  neither copy received
        1  only v1 received
        2  only v2 received
        3  both copies received and matched
        5  both received, mismatch — v1 selected
        6  both received, mismatch — v2 selected

    'With time diversity' reception: q ∈ {3, 5, 6}  (either copy arrived)
    'No time diversity' baseline (symmetric average of v1-primary and v2-primary):
        loss_nodiv = (n_v1_only + n_v2_only + 2 × n_neither) / (2 × N)

    Returns dict:
        quality        int  (N,)    base quality code
        t_quality      float (N,)   absolute time of each decoded group centre (s)
        both_recv      bool (N,)
        only_v1        bool (N,)
        only_v2        bool (N,)
        neither        bool (N,)
        rate_div       float        reception rate with time diversity
        rate_nodiv     float        reception rate without time diversity (avg)
        loss_div       float
        loss_nodiv     float
    Returns None on failure.
    """
    reader = TimeStampBasedReader(enable_gcs=False, gcs_channels=(1, 2, 3, 4))
    try:
        d         = _load_mat(mat_path, ['push1', 'databit1', 'changeofstrength1', 'packetnum1'])
        push      = np.asarray(d['push1'],                                             dtype=np.uint16).ravel()
        N0        = len(push)
        databit   = np.asarray(d.get('databit1',          np.zeros(N0, np.uint16)),   dtype=np.uint16).ravel()
        error     = np.asarray(d.get('changeofstrength1', np.zeros(N0, np.uint16)),   dtype=np.uint16).ravel()
        packetnum = np.asarray(d.get('packetnum1',        np.zeros(N0, np.uint16)),   dtype=np.uint16).ravel()
        plt.plot(packetnum)
        plt.show()
        # Arrays may arrive at different rates; the longest is the 100 kHz word clock.
        # Upsample shorter arrays by integer repeat to match.
        max_len = max(len(push), len(databit), len(error), len(packetnum))

        def _upsample(arr):
            if len(arr) == max_len:
                return arr
            ratio = round(max_len / len(arr))
            return np.repeat(arr, ratio)[:max_len]

        push      = _upsample(push)
        databit   = _upsample(databit)
        error     = _upsample(error)
        packetnum = _upsample(packetnum)

        # Reconstruct working_data words (inverse of _extract_channel_packets bit ops)
        words = ((databit & 0x1)
                 | ((packetnum & 0x7) << 4)
                 | ((error    & 0x1)  << 7)
                 | ((push     & 0x1)  << 8)).astype(np.uint16)

        # frame-level reception for step plots (computed before diversity combining)
        try:
            _st = float(_load_mat(mat_path, ['START_TIME']).get('START_TIME', 0.0))
        except Exception:
            _st = 0.0
        frame_received, t_frame = compute_frame_reception(words, _st)

        reader.decode_from_word_stream(words)
    except Exception as exc:
        print(f'  TimeStampBasedReader decode failed: {exc}')
        return None

    try:
        _values, quality_raw = reader.get_decoded_arrays(GCS_CHANNEL)
    except Exception as exc:
        print(f'  get_decoded_arrays failed: {exc}')
        return None

    quality_raw = np.asarray(quality_raw)
    quality = quality_raw.ravel().astype(int)   # flatten all 4 sub-samples per group → ~200 Hz          # keep error-flag bit (bit 3)
    q_base  = quality & 0x07                            # base code without error flag

    N         = len(quality)
    both_recv = np.isin(q_base, [3, 5, 6])
    only_v1   = q_base == 1
    only_v2   = q_base == 2
    neither   = q_base == 0

    out_rate = N / max_duration   # inferred decoded output rate (Hz)
    try:
        dd         = _load_mat(mat_path, ['START_TIME'])
        START_TIME = float(dd.get('START_TIME', 0.0))
    except Exception:
        START_TIME = 0.0
    t_quality = START_TIME + np.arange(N) / out_rate

    # with diversity: lost only when neither copy arrived
    loss_div   = float(neither.mean())
    rate_div   = 1.0 - loss_div
    # no diversity: symmetric average of v1-primary and v2-primary loss rates
    loss_nodiv = (int(only_v1.sum()) + int(only_v2.sum())
                  + 2 * int(neither.sum())) / (2 * N)
    rate_nodiv = 1.0 - loss_nodiv

    print(f'  Decoded {N} groups at ≈{out_rate:.0f} Hz')
    print(f'  both={both_recv.sum():5d}  v1_only={only_v1.sum():5d}  '
          f'v2_only={only_v2.sum():5d}  neither={neither.sum():5d}')
    print(f'  Recv: no-div={100*rate_nodiv:.1f}%  time-div={100*rate_div:.1f}%')

    return dict(quality=quality, t_quality=t_quality,
                both_recv=both_recv, only_v1=only_v1,
                only_v2=only_v2, neither=neither,
                rate_div=rate_div, rate_nodiv=rate_nodiv,
                loss_div=loss_div, loss_nodiv=loss_nodiv,
                frame_received=frame_received, t_frame=t_frame)


# ══════════════════════════════════════════════════════════════════════════════
# main
# ══════════════════════════════════════════════════════════════════════════════
def main():
    start_time   = float(sys.argv[1]) if len(sys.argv) > 1 else 8.0
    max_duration = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0

    labels    = ['Ant 1 only', 'Ant 2 only', 'Both antennas']
    mat_paths = [MAT_ANT1,    MAT_ANT2,    MAT_BOTH]
    step_clrs = ['#3366cc',   '#cc3333',   '#22aa44']

    # ── Per-window reception ───────────────────────────────────────────────────
    print('\n══ Per-window reception (push1) ══')
    rx     = []
    Fs_push = None
    WINDOW  = None
    for i, (lbl, mp) in enumerate(zip(labels, mat_paths)):
        print(f'\nRun {i+1}/3: {lbl}')
        r = compute_reception(mp, max_duration)
        rx.append(r)
        if r is not None and Fs_push is None:
            Fs_push = r['Fs_push']
            WINDOW  = r['WINDOW']

    # ── Time diversity decode ──────────────────────────────────────────────────
    td = [None, None, None]
    if HAS_TSBR:
        print('\n══ Time diversity decode (TimeStampBasedReader) ══')
        for i, (lbl, mp) in enumerate(zip(labels, mat_paths)):
            print(f'\n{lbl}:')
            td[i] = decode_time_diversity(mp, max_duration)

    # ── Raw IQ magnitude ───────────────────────────────────────────────────────
    print('\n══ Loading raw IQ ══')
    iq_start = start_time + SKIP_S
    iq_dur   = max(0.0, max_duration - SKIP_S)
    mag1_ds = mag2_ds = t_mag = None

    if iq_dur > 0:
        try:
            mag1 = read_iq_magnitude(IQ_FILE1, iq_start, iq_dur)
            mag2 = read_iq_magnitude(IQ_FILE2, iq_start, iq_dur)
            if WINDOW is not None and Fs_push is not None:
                WINDOW_IQ = max(1, round(WINDOW * FS_IQ / Fs_push))
                nWin_raw  = len(mag1) // WINDOW_IQ
                if nWin_raw > 0:
                    mag1_ds = (mag1[:nWin_raw * WINDOW_IQ]
                               .reshape(nWin_raw, WINDOW_IQ).mean(axis=1))
                    mag2_ds = (mag2[:nWin_raw * WINDOW_IQ]
                               .reshape(nWin_raw, WINDOW_IQ).mean(axis=1))
                    t_mag = (iq_start
                             + (np.arange(nWin_raw) * WINDOW_IQ + WINDOW_IQ / 2)
                             / FS_IQ)
                    print(f'  IQ WINDOW_IQ = {WINDOW_IQ} samples '
                          f'({1000*WINDOW_IQ/FS_IQ:.2f} ms),  {nWin_raw} windows')
        except Exception as exc:
            print(f'  WARNING: could not read IQ files: {exc}')

    # ══════════════════════════════════════════════════════════════════════════
    # Figure 1 — time-domain view
    # ══════════════════════════════════════════════════════════════════════════
    show_tsdiv = HAS_TSBR and td[2] is not None
    n_rows = 7 if show_tsdiv else 5

    fig1, axes = plt.subplots(n_rows, 1,
                              figsize=(13, 2.4 * n_rows),
                              sharex=True,
                              constrained_layout=True)

    # ── [0] Ant 1 |IQ| ────────────────────────────────────────────────────────
    ax = axes[0]
    if mag1_ds is not None:
        ax.plot(t_mag, mag1_ds, color='#7733bb', linewidth=0.7)
    else:
        ax.text(0.5, 0.5, 'IQ file unavailable',
                ha='center', va='center', transform=ax.transAxes)
    ax.set_yscale('log')
    ax.set_ylabel('|IQ|')
    ax.set_title('Antenna 1 — raw signal magnitude')
    ax.grid(True, linewidth=0.4)

    # ── [1] Ant 2 |IQ| ────────────────────────────────────────────────────────
    ax = axes[1]
    if mag2_ds is not None:
        ax.plot(t_mag, mag2_ds, color='#e07820', linewidth=0.7)
    else:
        ax.text(0.5, 0.5, 'IQ file unavailable',
                ha='center', va='center', transform=ax.transAxes)
    ax.set_yscale('log')
    ax.set_ylabel('|IQ|')
    ax.set_title('Antenna 2 — raw signal magnitude')
    ax.grid(True, linewidth=0.4)

    # ── [2–4] Per-sample reception (step plots) ───────────────────────────────
    for i in range(3):
        ax = axes[i + 2]
        if HAS_TSBR and td[i] is not None and FRAME_LEVEL_STEP_PLOTS:
            # Raw frame-level reception (~400 Hz), v1 and v2 at their actual times
            fr  = td[i]['frame_received']
            t_f = td[i]['t_frame']
            ax.step(t_f, fr.astype(float),
                    where='post', color=step_clrs[i], linewidth=0.8)
            ax.set_ylim(-0.15, 1.3)
            ax.set_yticks([0, 1])
            ax.set_yticklabels(['miss', 'ok'])
            n_recv = int(fr.sum())
            n_tot  = len(fr)
            ax.set_title(
                f'{labels[i]}  —  {100*fr.mean():.1f}% packets in-sequence '
                f'({n_recv} / {n_tot} frames)',
                fontsize=9)
        elif HAS_TSBR and td[i] is not None:
            q_all    = np.asarray(td[i]['quality'])
            t_q_i    = np.asarray(td[i]['t_quality'])
            q_base_i = q_all & 0x07
            q_err_i  = (q_all & 0x08) != 0
            received_ok = np.isin(q_base_i, [1, 3, 5]) & ~q_err_i
            ax.step(t_q_i, received_ok.astype(float),
                    where='post', color=step_clrs[i], linewidth=0.8)
            ax.set_ylim(-0.15, 1.3)
            ax.set_yticks([0, 1])
            ax.set_yticklabels(['miss', 'ok'])
            n_recv = int(received_ok.sum())
            n_tot  = len(received_ok)
            ax.set_title(
                f'{labels[i]}  —  {100*received_ok.mean():.1f}% v1 in-sequence '
                f'({n_recv} / {n_tot} samples)',
                fontsize=9)
        elif rx[i] is not None and len(rx[i]['received']) > 0:
            r = rx[i]
            ax.step(r['t_windows'], r['received'].astype(float),
                    where='post', color=step_clrs[i], linewidth=0.8)
            ax.set_ylim(-0.15, 1.3)
            ax.set_yticks([0, 1])
            ax.set_yticklabels(['miss', 'ok'])
            n_recv = int(r['received'].sum())
            n_tot  = len(r['received'])
            ax.set_title(f'{labels[i]}  —  {100*r["rate"]:.1f}% received '
                         f'({n_recv} / {n_tot} windows)', fontsize=9)
        else:
            ax.text(0.5, 0.5, 'No data / no valid onset',
                    ha='center', va='center', transform=ax.transAxes)
            ax.set_title(labels[i])
        ax.grid(True, linewidth=0.4)

    # ── [5] Time-diversity quality colour strip (both antennas) ───────────────
    # ── [6] Quality scatter plot (individual samples) ─────────────────────────
    if show_tsdiv:
        tdb   = td[2]
        q     = np.asarray(tdb['quality'])
        t_q   = np.asarray(tdb['t_quality'])
        q_base = q & 0x07
        q_err  = (q & 0x08) != 0

        # 4-category mapping:  0=neither(red)  1=anomaly(orange)  2=one copy(yellow)  3=both(green)
        idx = np.zeros(len(q), dtype=np.uint8)
        idx[q_err & (q_base != 0)]               = 1   # received but flagged
        idx[np.isin(q_base, [1, 2]) & ~q_err]    = 2   # one copy, clean
        idx[np.isin(q_base, [3, 5, 6]) & ~q_err] = 3   # both copies, clean

        CAT_COLORS_Q = ['#cc2222', '#e07820', '#e0b030', '#22aa44']
        cmap  = mcolors.ListedColormap(CAT_COLORS_Q)
        norm  = mcolors.BoundaryNorm([-.5, .5, 1.5, 2.5, 3.5], cmap.N)

        # ── colour strip (imshow) ──────────────────────────────────────────────
        ax  = axes[5]
        dt  = ((t_q[-1] - t_q[0]) / max(len(t_q) - 1, 1)
               if len(t_q) > 1 else max_duration / max(len(t_q), 1))
        extent = [t_q[0] - dt / 2, t_q[-1] + dt / 2, 0, 1]
        ax.imshow(np.tile(idx, (30, 1)),
                  aspect='auto', extent=extent, origin='lower',
                  cmap=cmap, norm=norm, interpolation='nearest')
        ax.set_yticks([])

        patches = [
            mpatches.Patch(color='#cc2222', label='Neither received  (q=0)'),
            mpatches.Patch(color='#e07820', label='Received + anomaly flag'),
            mpatches.Patch(color='#e0b030', label='One copy only  (q=1 or 2)'),
            mpatches.Patch(color='#22aa44', label='Both copies  (q=3,5,6)'),
        ]
        ax.legend(handles=patches, loc='upper right', fontsize=7, framealpha=0.85)
        ax.set_title(
            f'Both antennas — time-diversity outcome    '
            f'no-div {100*tdb["rate_nodiv"]:.1f}%  →  '
            f'time-div {100*tdb["rate_div"]:.1f}%',
            fontsize=9)
        ax.grid(True, linewidth=0.4)

        # ── scatter plot (individual samples) ──────────────────────────────────
        ax6 = axes[6]
        dot_colors = [CAT_COLORS_Q[i] for i in idx]
        ax6.scatter(t_q, np.zeros(len(t_q)), c=dot_colors,
                    marker='|', s=300, linewidths=1.5)
        ax6.set_ylim(-0.5, 0.5)
        ax6.set_yticks([])
        ax6.set_title('Quality per decoded sample  (same colour key as strip above)', fontsize=9)
        ax6.grid(True, linewidth=0.4)

    axes[-1].set_xlabel('Time (s)')
    axes[0].set_xlim(left=start_time + SKIP_S)
    fig1.suptitle(
        f'Packet reception  —  t₀ = {start_time:.1f} s, '
        f'duration = {max_duration:.2f} s',
        fontsize=11)

    # ══════════════════════════════════════════════════════════════════════════
    # Figure 2 — reception-rate bar chart
    # ══════════════════════════════════════════════════════════════════════════
    fig2, ax2 = plt.subplots(figsize=(10, 5))

    x     = np.arange(3)
    width = 0.3

    if HAS_TSBR and any(t is not None for t in td):
        nodiv_rates  = [td[i]['rate_nodiv'] if td[i] else 0.0 for i in range(3)]
        timdiv_rates = [td[i]['rate_div']   if td[i] else 0.0 for i in range(3)]

        bars_nodiv = ax2.bar(x - width / 2, [100 * v for v in nodiv_rates],
                             width, label='No time diversity  (avg single-copy baseline)',
                             color='#e07820', alpha=0.85, zorder=3)
        bars_tdiv  = ax2.bar(x + width / 2, [100 * v for v in timdiv_rates],
                             width, label='With time diversity',
                             color='#22aa44', alpha=0.85, zorder=3)

        for bars in (bars_nodiv, bars_tdiv):
            for b in bars:
                h = b.get_height()
                ax2.text(b.get_x() + b.get_width() / 2, h + 0.5,
                         f'{h:.0f}%', ha='center', va='bottom', fontsize=8)

    ax2.set_xticks(x)
    ax2.set_xticklabels(labels)
    ax2.set_ylabel('Packet reception rate (%)')
    ax2.set_ylim(0, 115)
    ax2.set_title(
        f'Reception rate by antenna config and time diversity  '
        f'(t₀ = {start_time:.1f} s, duration = {max_duration:.2f} s)')
    ax2.legend(loc='lower right', fontsize=9)
    ax2.grid(axis='y', alpha=0.35, zorder=0)
    fig2.tight_layout()

    plt.show()


if __name__ == '__main__':
    main()