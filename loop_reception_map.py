#!/usr/bin/env python3
"""
loop_reception_map.py

Loads a single long MAT file covering the full two-lap walk, runs TSBR
decode, computes sliding-window reception rates, and produces:

  Figure 1 — rolling reception rate vs. recording time  (no-div + time-div)
  Figure 2 — room walk diagram with path colored by local time-div rate

Usage:
    python loop_reception_map.py [mat_file]

Set WALK_START_S, WALK_END_S, and WALK_CORNER_TIMES_7 below to match the
recording timestamps (seconds) at the start, corners, and end of the walk.
"""

import os, sys
import numpy as np
import scipy.io
import matplotlib.pyplot as plt
import matplotlib.collections as mc
import matplotlib.colors as mcolors
import matplotlib.patheffects as mpe

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

# Long MAT files (one per antenna configuration) covering the full walk.
# Set a path to None to skip that configuration.
MAT_ANT1 = '/mnt/c/Temp/stream_out_ant1only_wholefile.mat'
MAT_ANT2 = '/mnt/c/Temp/stream_out_ant2only_wholefile.mat'
MAT_BOTH = '/mnt/c/Temp/stream_out_both_wholefile.mat'

# Companion walk video (shared by all three recordings). Used only as a fallback
# wall-clock anchor for the q-value CSV when a MAT lacks a WALL_START variable:
# the video's start time (ffprobe creation_time, else file mtime − duration)
# anchors sample 0. Set to None to skip the fallback.
WALK_VIDEO =  '/mnt/g/Shared drives/Spontaneous_EEG_paper/Data/characterization/wireless_link/0601_longloopvideo.MOV'

GCS_CHANNEL = '2'

# Sliding window: number of TSBR output samples (~200 Hz) to average over.
# 150 ≈ 0.75 s ≈ 0.5–1 m of walk.  Increase for smoother, decrease for sharper.
WINDOW_SAMPLES = 300

# Recording time (seconds) at the start and end of the walk
WALK_START_S = 0.0
WALK_END_S   = None   # None → use full recording duration

# Recording times at the 7 intermediate path corners (seconds), in order:
#   BR→BL,  BL→TL,  TL→TR,  TR→BR,  BR→TR,  TR→TL,  TL→BL
# Set any unknown corner to None for arc-proportional interpolation.
WALK_CORNER_TIMES_7 = [i-25 for i in [37, 46, 58, 64, 71, 82, 90]]

# Room geometry (feet → metres)
W_M  = 48 * 0.3048
H_M  = 28 * 0.3048
RX_M = (W_M / 2, 3 * 0.3048)

# Lateral separation (m) between the two laps' device positions. This is PHYSICAL,
# not cosmetic: the antenna is head-mounted, so walking one direction vs. the other
# places the head ~1 foot to the side — the two laps sample genuinely different
# positions. ≈ 1 ft = 0.3048 m.
DELTA_PATH = 0.3048

# Path colormap for reception rate. Viridis  is perceptually uniform, optimised
# for colour-vision deficiency, and grayscale-safe (unlike RdYlGn).
PATH_CMAP = plt.cm.viridis

# Overlay colours — chosen to sit OFF the viridis  colormap (which spans
# blue→yellow) so direction arrows / landmarks never read as a reception value.
# Warm hues (orange, magenta, crimson) are all off-scale.
CW_COLOR    = 'darkorange'  # Lap 1, clockwise
CCW_COLOR   = 'magenta'     # Lap 2, counter-clockwise
START_COLOR = 'white'       # Start/End diamond (black-edged; limegreen blends w/ viridis)
RX_COLOR    = 'crimson'     # Rx star

LABELS   = ['Ant 1 only', 'Ant 2 only', 'Both antennas']

# Every figure is saved here (PNG) for review before you choose which to keep.
PLOTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'plots')

# Decoder quality base-code (quality & 0x07) → human-readable word for the CSV.
#   0 none | 1 only_v1 | 2 only_v2 | 3 both (copies matched)
#   5 mismatch_v1 / 6 mismatch_v2 (copies disagreed; decoder picked v1 / v2).
# The chosen copy is denoted so the wrong-copy half of a mismatch can be
# attributed (a mismatch is 50% correct under no-diversity).
# Bit 3 (0x08) is a separate error flag and is stripped before this lookup.
# Codes 4 and 7 are never produced by the decoder.
Q_WORDS = {0: 'none', 1: 'only_v1', 2: 'only_v2', 3: 'both',
           5: 'mismatch_v1', 6: 'mismatch_v2'}


# ══════════════════════════════════════════════════════════════════════════════
# Path geometry helpers  (kept in sync with survey_diversity_plot.py)
# ══════════════════════════════════════════════════════════════════════════════

def _perimeter_arc_anchors(W, H):
    return np.array([0, W, W+H, 2*W+H, 2*(W+H),
                     2*W+3*H, 3*W+3*H, 3*W+4*H, 4*(W+H)])


def _perimeter_xy(s, W, H):
    H, W, s = float(H), float(W), float(s)
    if s <= W:               return (W - s, 0.0)
    elif s <= W + H:         return (0.0, s - W)
    elif s <= 2*W + H:       return (s - W - H, H)
    elif s <= 2*(W + H):     return (W, H - (s - 2*W - H))
    elif s <= 2*W + 3*H:     return (W, s - 2*(W + H))
    elif s <= 3*W + 3*H:     return (W - (s - 2*W - 3*H), H)
    elif s <= 3*W + 4*H:     return (0.0, H - (s - 3*W - 3*H))
    else:                    return (s - 3*W - 4*H, 0.0)


def _fill_corner_times(t_waypoints_raw, W, H):
    arcs   = _perimeter_arc_anchors(W, H)
    t_wpts = list(t_waypoints_raw)
    known  = [(i, float(t_wpts[i])) for i in range(len(t_wpts))
              if t_wpts[i] is not None]
    for k in range(len(known) - 1):
        lo_i, t_lo = known[k]
        hi_i, t_hi = known[k + 1]
        arc_span = arcs[hi_i] - arcs[lo_i]
        for i in range(lo_i + 1, hi_i):
            frac = (arcs[i] - arcs[lo_i]) / arc_span
            t_wpts[i] = t_lo + frac * (t_hi - t_lo)
    return t_wpts


def _position_and_arc(t, t_waypoints_raw, W, H):
    t_wpts = _fill_corner_times(t_waypoints_raw, W, H)
    arcs   = _perimeter_arc_anchors(W, H)
    t = max(float(t_wpts[0]), min(float(t), float(t_wpts[-1])))
    for i in range(len(t_wpts) - 1):
        dt = float(t_wpts[i + 1]) - float(t_wpts[i])
        if t <= float(t_wpts[i + 1]) + 1e-9:
            frac = (t - float(t_wpts[i])) / dt if dt > 0 else 0.0
            s = float(arcs[i]) + frac * (float(arcs[i + 1]) - float(arcs[i]))
            return _perimeter_xy(s, W, H), s
    s = float(arcs[-1])
    return _perimeter_xy(s, W, H), s


def _path_xy_offset(s, W, H, delta):
    """W×H are the given path dimensions (outer CCW loop).
    Lap 1 CW: delta inside the W×H boundary.
    Lap 2 CCW: exactly at the W×H boundary."""
    H, W, s, delta = float(H), float(W), float(s), float(delta)
    perimeter = 2 * (W + H)
    # Lap 1 CW — inner path (exact corners at delta inside W×H)
    if s <= W:               return (W - delta - s * (W - 2*delta) / W,           delta)
    elif s <= W + H:         return (delta,                                         delta + (s - W) * (H - 2*delta) / H)
    elif s <= 2*W + H:       return (delta + (s - W - H) * (W - 2*delta) / W,     H - delta)
    elif s <= perimeter:     return (W - delta,                                     H - delta - (s - 2*W - H) * (H - 2*delta) / H)
    # Lap 2 CCW — outer path at exactly W×H (no offset)
    elif s <= 2*W + 3*H:     return (W,                   s - perimeter)
    elif s <= 3*W + 3*H:     return (W - (s - 2*W - 3*H), H)
    elif s <= 3*W + 4*H:     return (0.0,                  H - (s - 3*W - 3*H))
    else:                    return (s - 3*W - 4*H,        0.0)


def _save_all_figures(out_dir):
    """Save every open matplotlib figure to out_dir as PNG, named from its
    suptitle so they are easy to pick through afterward."""
    import re
    os.makedirs(out_dir, exist_ok=True)
    saved = []
    for num in plt.get_fignums():
        fig = plt.figure(num)
        title = (fig.get_suptitle() or '').strip()
        slug = re.sub(r'[^\w\-]+', '_', title).strip('_')[:60]
        name = f'fig{num:02d}_{slug}' if slug else f'fig{num:02d}'
        path = os.path.join(out_dir, name + '.png')
        fig.savefig(path, dpi=150, bbox_inches='tight')
        saved.append(os.path.basename(path))
    print(f'\nSaved {len(saved)} figure(s) to {out_dir}/')
    for nm in saved:
        print(f'  {nm}')
    return saved


def _add_path_arrow(ax, p0, p1, color, frac=0.4):
    """Stemless direction marker: a filled triangle (no shaft) pointing along
    p0→p1, so direction is conveyed while covering as little of the colored
    path as possible. White edge keeps it legible over any background colour."""
    p0, p1 = np.asarray(p0, float), np.asarray(p1, float)
    d   = p1 - p0
    mid = p0 + frac * d
    # Regular-triangle marker (3 sides) points +y at angle 0; rotate to heading.
    angle = np.degrees(np.arctan2(d[1], d[0])) - 90.0
    ax.scatter([mid[0]], [mid[1]], marker=(3, 0, angle), s=240,
               facecolor=color, edgecolor='white', linewidths=1.1, zorder=6)


# ══════════════════════════════════════════════════════════════════════════════
# Data loading & decoding
# ══════════════════════════════════════════════════════════════════════════════

def _load_mat(filepath, varnames):
    try:
        d = scipy.io.loadmat(filepath, squeeze_me=True, variable_names=list(varnames))
        return {k: d[k] for k in varnames if k in d}
    except NotImplementedError:
        pass
    except Exception as exc:
        raise RuntimeError(f'scipy.io.loadmat failed: {exc}') from exc
    try:
        import h5py
    except ImportError as exc:
        raise RuntimeError('Mat file is MATLAB v7.3 (HDF5) but h5py is not installed. '
                           'Install with: pip install h5py') from exc
    result = {}
    with h5py.File(filepath, 'r') as f:
        for k in varnames:
            if k not in f:
                continue
            raw = f[k][()]
            result[k] = raw.ravel().astype(np.float64) if raw.ndim > 0 else float(raw)
    return result


def _read_wall_times(filepath):
    """Read the WALL_START / WALL_END string vars from a MAT file.

    Handles both classic (scipy) and v7.3/HDF5 (h5py, where char arrays are
    uint16 code points) layouts. Returns (start, end) as datetime objects, or
    None for any that is missing/unparseable. Format: 'YYYY-MM-DD HH:MM:SS'.
    """
    from datetime import datetime
    raw = {}
    try:
        d = scipy.io.loadmat(filepath, squeeze_me=True,
                             variable_names=['WALL_START', 'WALL_END'])
        for k in ('WALL_START', 'WALL_END'):
            if k in d:
                raw[k] = d[k]
    except NotImplementedError:
        import h5py
        with h5py.File(filepath, 'r') as f:
            for k in ('WALL_START', 'WALL_END'):
                if k in f:
                    raw[k] = ''.join(chr(int(c)) for c in np.ravel(f[k][()]))
    except Exception:
        return None, None

    def _parse(k):
        if k not in raw:
            return None
        v = raw[k]
        if isinstance(v, np.ndarray):
            v = v.item() if v.size == 1 else ''.join(map(str, v.tolist()))
        s = str(v).strip()
        if not s:
            return None
        try:
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            print(f'  WARN: could not parse {k}={s!r} (expected "YYYY-MM-DD HH:MM:SS")')
            return None

    return _parse('WALL_START'), _parse('WALL_END')


def _video_start_time(video_path):
    """Best-effort wall-clock start time of a video, as a local naive datetime.

    Prefers ffprobe's creation_time (the recording start; converted from UTC to
    local). Falls back to file mtime minus the video duration (mtime ≈ when the
    file was finalized = end of recording), then to bare mtime. Returns None if
    the path is missing or nothing can be determined.
    """
    import subprocess, json
    from datetime import datetime, timedelta
    if not video_path or not os.path.exists(video_path):
        if video_path:
            print(f'  Video fallback path not found: {video_path}')
        return None

    creation = duration = None
    try:
        out = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-print_format', 'json',
             '-show_entries', 'format_tags=creation_time:format=duration',
             video_path],
            capture_output=True, text=True, timeout=30)
        if out.returncode == 0 and out.stdout.strip():
            fmt = json.loads(out.stdout).get('format', {})
            if fmt.get('duration'):
                duration = float(fmt['duration'])
            ct = (fmt.get('tags') or {}).get('creation_time')
            if ct:
                dt = datetime.fromisoformat(ct.replace('Z', '+00:00'))
                creation = dt.astimezone().replace(tzinfo=None) if dt.tzinfo else dt
    except (FileNotFoundError, subprocess.SubprocessError, ValueError, json.JSONDecodeError):
        pass  # ffprobe absent or metadata unreadable — fall through to mtime

    base = os.path.basename(video_path)
    if creation is not None:
        print(f'  Video start (ffprobe creation_time): {creation:%Y-%m-%d %H:%M:%S}  [{base}]')
        return creation
    mtime = datetime.fromtimestamp(os.path.getmtime(video_path))
    if duration is not None:
        start = mtime - timedelta(seconds=duration)
        print(f'  Video start (mtime − {duration:.0f}s duration): {start:%Y-%m-%d %H:%M:%S}  [{base}]')
        return start
    print(f'  Video start (mtime, approximate): {mtime:%Y-%m-%d %H:%M:%S}  [{base}]')
    return mtime


def save_quality_timeseries(mat_path, quality, output_rate, video_path=None):
    """Write a CSV of per-sample quality bytes with wall-clock timestamps.

    Timestamps are anchored at WALL_START (read from the MAT) plus the decoded
    elapsed time (sample_index / output_rate) — WALL_END is used only as a
    sanity check, since the recording may have been stopped slightly early.
    If the MAT has no WALL_START, falls back to the companion video's start time.
    Saved next to the MAT file as <matbasename>_qvalues.csv.
    """
    wall_start, wall_end = _read_wall_times(mat_path)
    anchor_source = 'WALL_START'
    if wall_start is None and video_path is not None:
        wall_start = _video_start_time(video_path)
        anchor_source = 'video'
    n = len(quality)
    rel = np.arange(n) / float(output_rate)

    if wall_start is not None:
        base = np.datetime64(wall_start.strftime('%Y-%m-%dT%H:%M:%S'))
        ts = base + np.round(rel * 1000).astype('timedelta64[ms]')
        # datetime64 renders ISO with a 'T'; use a space to match WALL_START format.
        wall_strs = np.char.replace(ts.astype('datetime64[ms]').astype(str), 'T', ' ')
        computed_end = wall_start.strftime('%Y-%m-%d %H:%M:%S') if n == 0 else \
            str(ts[-1].astype('datetime64[s]')).replace('T', ' ')
        note = f'anchor={anchor_source}({wall_start:%Y-%m-%d %H:%M:%S})  computed_end={computed_end}'
        if anchor_source == 'WALL_START' and wall_end is not None:
            note += f'  WALL_END={wall_end:%Y-%m-%d %H:%M:%S}'
    else:
        wall_strs = np.full(n, '', dtype=object)
        note = 'no WALL_START and no video anchor — wall_time column left blank'

    # Strip the error flag (bit 3) before mapping the base reception code.
    q_words = np.array([Q_WORDS.get(int(q) & 0x07, f'q{int(q) & 0x07}') for q in quality],
                       dtype=object)
    out_path = os.path.splitext(mat_path)[0] + '_qvalues.csv'
    arr = np.column_stack([
        np.asarray(wall_strs, dtype=object),
        np.round(rel, 4).astype(str),
        q_words,
    ])
    np.savetxt(out_path, arr, fmt='%s', delimiter=',',
               header='wall_time,rel_s,q', comments='')
    print(f'  Saved {n} q-values → {out_path}  ({note})')
    return out_path


def decode_full_file(mat_path):
    """
    Load the long MAT, reconstruct the word stream, run TSBR decode.
    Returns (quality, output_rate_hz) where quality is a per-sample int array.
    """
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from sdr_reader_gcs_write import TimeStampBasedReader

    reader = TimeStampBasedReader(
        enable_gcs=False,
        quiet=True,  # suppress per-event decode warnings; we print a summary below
        gcs_channels=(1, 2, 3, 4),
        accepted_frame_lengths=(250, 248),
        frame_length_counts={250: 18, 248: 1},
    )

    d = _load_mat(mat_path, ['push1', 'databit1', 'changeofstrength1', 'packetnum1'])
    push      = np.asarray(d['push1'],                                           dtype=np.uint16).ravel()
    N0        = len(push)
    databit   = np.asarray(d.get('databit1',          np.zeros(N0, np.uint16)), dtype=np.uint16).ravel()
    error     = np.asarray(d.get('changeofstrength1', np.zeros(N0, np.uint16)), dtype=np.uint16).ravel()
    packetnum = np.asarray(d.get('packetnum1',        np.zeros(N0, np.uint16)), dtype=np.uint16).ravel()

    max_len = max(len(push), len(databit), len(error), len(packetnum))

    def _align(arr):
        if len(arr) == max_len:
            return arr
        ratio = round(max_len / len(arr))
        return np.repeat(arr, ratio)[:max_len]
    push, databit, error, packetnum = (_align(a) for a in (push, databit, error, packetnum))

    words = ((databit    & 0x1)
             | ((packetnum & 0x7) << 4)
             | ((error    & 0x1)  << 7)
             | ((push     & 0x1)  << 8)).astype(np.uint16)

    reader.decode_from_word_stream(words)
    _values, quality_raw = reader.get_decoded_arrays(GCS_CHANNEL)
    quality = np.asarray(quality_raw).ravel().astype(int)

    # Quality-byte distribution. The error flag (bit 3, 0x08) is stripped first;
    # we categorise on the base code (quality & 0x07). mismatch (base 5/6) is its
    # own category — both copies arrived but disagreed and the decoder picked one
    # by neighbour/magnitude; it is a recovered sample, not a miss.
    q_base = np.asarray(quality).astype(int) & 0x07
    n = len(q_base)
    n_error = int(np.count_nonzero(np.asarray(quality).astype(int) & 0x08))
    cols   = ['none', 'only_v1', 'only_v2', 'both', 'mismatch']
    counts = {
        'none':     int(np.count_nonzero(q_base == 0)),
        'only_v1':  int(np.count_nonzero(q_base == 1)),
        'only_v2':  int(np.count_nonzero(q_base == 2)),
        'both':     int(np.count_nonzero(q_base == 3)),
        'mismatch': int(np.count_nonzero(np.isin(q_base, (5, 6)))),
    }
    def _pct(c):
        return 100.0 * c / n if n else 0.0
    print(f'  Decoded {n} samples @ {reader.output_rate_hz:.1f} Hz '
          f'({n / reader.output_rate_hz:.1f}s)')
    print('  ' + f'{"":8}' + ''.join(f'{c:>10}' for c in cols))
    print('  ' + f'{"count":8}' + ''.join(f'{counts[c]:>10}' for c in cols))
    print('  ' + f'{"pct":8}' + ''.join(f'{_pct(counts[c]):>9.1f}%' for c in cols))
    print(f'  (error flag set on {n_error} / {n} samples = {_pct(n_error):.1f}%)')
    accounted = sum(counts.values())
    if accounted != n:
        print(f'  NOTE: {n - accounted} sample(s) had an unexpected base code '
              f'(not in {{0,1,2,3,5,6}})')

    # Mismatch / phase-tracking stats.
    print(f'  Mismatch stats — anomalies:{reader.packet_sequence_anomaly_count}  '
          f'header_drops:{reader.packet_sequence_header_drops}  '
          f'dist_corrections:{reader.packet_num_distance_corrections}  '
          f'relocks:{reader.phase_relocks}  locked:{reader._phase_locked}')

    return quality, float(reader.output_rate_hz)


# ══════════════════════════════════════════════════════════════════════════════
# Sliding-window rates
# ══════════════════════════════════════════════════════════════════════════════

def sliding_rates(quality, W):
    """
    Centered sliding window of width W over quality bytes.
    Uses cumsum rolling mean so edge samples clip to a smaller window
    rather than zero-padding, which would artificially lower rates at the
    start and end of the recording.

    Per-sample contribution to each rate (base code = quality & 0x07):
        both(3)        → nodiv 1,   timdiv 1
        only_v1(1)/only_v2(2) → nodiv ½, timdiv 1   (one of two copies present)
        mismatch(5/6)  → nodiv ½,   timdiv 1   (both copies present but disagree;
                          exactly one single-copy system holds the correct value,
                          so it is 50% correct without diversity, but diversity
                          picks the right one → full credit)
        none(0)        → nodiv 0,   timdiv 0

    Returns:
        rate_nodiv:  1 – (only_v1 + only_v2 + mismatch + 2·neither) / 2
        rate_timdiv: 1 – neither_fraction
    Both shape (N,), dtype float64.
    """
    q_base   = quality & 0x07
    neither  = (q_base == 0).astype(float)
    only_v1  = (q_base == 1).astype(float)
    only_v2  = (q_base == 2).astype(float)
    mismatch = np.isin(q_base, (5, 6)).astype(float)

    def _rolling(arr):
        half = W // 2
        n    = len(arr)
        cs   = np.concatenate([[0.0], np.cumsum(arr)])
        lo   = np.maximum(0, np.arange(n) - half)
        hi   = np.minimum(n, np.arange(n) + half + 1)
        return (cs[hi] - cs[lo]) / (hi - lo).astype(float)

    s_neither  = _rolling(neither)
    s_v1       = _rolling(only_v1)
    s_v2       = _rolling(only_v2)
    s_mismatch = _rolling(mismatch)

    rate_timdiv = 1.0 - s_neither
    rate_nodiv  = 1.0 - (s_v1 + s_v2 + s_mismatch + 2.0 * s_neither) / 2.0
    return rate_nodiv, rate_timdiv


# ══════════════════════════════════════════════════════════════════════════════
# Figure 1 — rate vs time (3 subplots, one per antenna config)
# ══════════════════════════════════════════════════════════════════════════════

def plot_rate_vs_time_all(configs, walk_start, walk_end, window_s):
    """
    configs: list of (label, t_axis, rate_nodiv, rate_timdiv) — skip Nones.
    Produces one subplot per config, shared x-axis.
    """
    active = [(lbl, t, nd, td) for lbl, t, nd, td in configs
              if t is not None]
    n = len(active)
    fig, axes = plt.subplots(n, 1, figsize=(13, 3 * n), sharex=True,
                             constrained_layout=True)
    if n == 1:
        axes = [axes]

    for ax, (label, t_axis, rate_nodiv, rate_timdiv) in zip(axes, active):
        ax.fill_between(t_axis, 100 * rate_timdiv, alpha=0.15, color='#22aa44')
        ax.plot(t_axis, 100 * rate_timdiv,
                color='#22aa44', linewidth=1.1, label='With time diversity')

        ax.fill_between(t_axis, 100 * rate_nodiv, alpha=0.15, color='#e07820')
        ax.plot(t_axis, 100 * rate_nodiv,
                color='#e07820', linewidth=1.1, label='No time diversity (avg single-copy)')

        walk_mask = (t_axis >= walk_start) & (t_axis <= walk_end)
        if walk_mask.any():
            m_td = 100 * float(np.nanmean(rate_timdiv[walk_mask]))
            m_nd = 100 * float(np.nanmean(rate_nodiv[walk_mask]))
            ax.axhline(m_td, color='#22aa44', linestyle='--', linewidth=0.9, alpha=0.7)
            ax.axhline(m_nd, color='#e07820', linestyle='--', linewidth=0.9, alpha=0.7)
            ax.text(walk_end, m_td + 1.5, f'{m_td:.0f}%',
                    color='#22aa44', ha='right', va='bottom', fontsize=9)
            ax.text(walk_end, m_nd - 1.5, f'{m_nd:.0f}%',
                    color='#e07820', ha='right', va='top', fontsize=9)

        ax.axvspan(walk_start, walk_end, alpha=0.07, color='steelblue')
        ax.set_xlim(t_axis[0], t_axis[-1])
        ax.set_ylim(0, 105)
        ax.set_ylabel('Reception rate (%)')
        ax.set_title(label)
        ax.grid(True, alpha=0.35)

    axes[-1].set_xlabel('Recording time (s)')
    # Single shared legend outside the axes (the curves are identical across
    # subplots), so it never overlaps the data.
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='outside lower center', ncol=2, fontsize=9)
    fig.suptitle(
        f'Sliding-window reception rate  '
        f'(W = {WINDOW_SAMPLES} samples ≈ {window_s:.2f} s)',
        fontsize=11)
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# Figure 2 — colored paths (one room diagram per antenna config)
# ══════════════════════════════════════════════════════════════════════════════

def _draw_room_path(ax, t_walk, rate_timdiv_walk, t_waypoints_raw,
                    W, H, rx, delta, label):
    """Populate one axes with a colored-path room diagram."""
    cmap = PATH_CMAP
    norm = mcolors.Normalize(vmin=0, vmax=1)

    arcs_path = np.array([_position_and_arc(t, t_waypoints_raw, W, H)[1]
                          for t in t_walk])
    xy_path = np.array([_path_xy_offset(s, W, H, delta) for s in arcs_path])

    segs   = np.stack([xy_path[:-1], xy_path[1:]], axis=1)
    colors = cmap(norm(rate_timdiv_walk[:-1]))
    lc = mc.LineCollection(segs, colors=colors, linewidths=3.5,
                           capstyle='round', joinstyle='round', zorder=5)

    ax.set_aspect('equal')
    ax.set_xlabel('x  [m]', fontsize=8)
    ax.set_ylabel('y  [m]', fontsize=8)
    ax.set_title(label, fontsize=9)

    cw_xy  = np.array([[W-delta, delta], [delta, delta],
                        [delta, H-delta], [W-delta, H-delta], [W-delta, delta]])
    ccw_xy = np.array([[W, 0.0], [W, H], [0.0, H], [0.0, 0.0], [W, 0.0]])
    ax.plot(cw_xy[:, 0],  cw_xy[:, 1],  '-',  color=CW_COLOR,
            lw=1.0, alpha=0.25)
    ax.plot(ccw_xy[:, 0], ccw_xy[:, 1], '--', color=CCW_COLOR,
            lw=1.0, alpha=0.25)
    for i in range(4):
        _add_path_arrow(ax, cw_xy[i],  cw_xy[i + 1],  CW_COLOR,  frac=0.4)
        _add_path_arrow(ax, ccw_xy[i], ccw_xy[i + 1], CCW_COLOR, frac=0.4)

    ax.add_collection(lc)
    ax.plot(*_path_xy_offset(0.0, W, H, delta), 'D', markersize=10,
            color=START_COLOR, markeredgecolor='black', markeredgewidth=0.8,
            zorder=8, label='Start / End')
    ax.plot(rx[0], rx[1], '*', markersize=18, color=RX_COLOR,
            markeredgecolor='white', markeredgewidth=0.7, zorder=8, label='Rx')
    ax.text(rx[0] + 0.15, rx[1] + 0.35, 'Rx',
            fontsize=8, fontweight='bold', color=RX_COLOR)

    ax.set_xlim(-0.4, W + 0.4)
    ax.set_ylim(-0.4, H + 0.4)
    ax.tick_params(labelsize=7)
    return lc


# Okabe–Ito colour-blind-safe categorical palette, one colour per antenna config.
# High-contrast picks (vermillion over pale orange) so the three series separate.
COND_COLORS = {
    'Ant 1 only':    '#0072B2',  # blue
    'Ant 2 only':    '#D55E00',  # vermillion
    'Both antennas': '#009E73',  # bluish green
}


def _auto_distance_zones(dists, n_zones=4):
    """Split pooled distances into n_zones ordered bands at the largest gaps in
    the sorted distribution. The room perimeter only sits at a few distances from
    a fixed Rx, so distance is effectively clumpy — splitting at the natural gaps
    recovers those bands. Returns bin edges (length n_zones+1)."""
    d = np.sort(np.asarray(dists, float))
    d = d[np.isfinite(d)]
    if len(d) < 2:
        lo = d[0] if len(d) else 0.0
        return np.array([lo - 1e-6, lo + 1e-6])
    n_zones = max(1, min(n_zones, len(d) - 1))
    if n_zones == 1:
        return np.array([d[0] - 1e-6, d[-1] + 1e-6])
    split_at = np.sort(np.argsort(np.diff(d))[-(n_zones - 1):])  # largest gaps
    bounds = [(d[i] + d[i + 1]) / 2.0 for i in split_at]
    return np.array([d[0] - 1e-6, *bounds, d[-1] + 1e-6])


def plot_rate_vs_distance(configs, t_waypoints_raw, W, H, rx, delta, n_zones=4):
    """Reception rate aggregated into distance zones, one series per antenna
    config — a readable replacement for the raw scatter.

    Distance from Rx is binned into ~n_zones natural bands (the perimeter clumps
    at a few distances). Per zone × config the three configs are dodged apart and
    shown as: time-diversity median ± IQR (filled marker, solid line across
    zones) and no-diversity median (hollow marker, dashed line). The vertical
    segment joining them is the time-diversity gain.

    configs: (label, t_walk, rate_timdiv_walk, rate_nodiv_walk) — skip Nones.
    """
    active = [c for c in configs if c[1] is not None]
    rx = np.asarray(rx, float)

    # Map each config's samples to distance-from-Rx; pool distances for zoning.
    series, all_d = [], []
    for label, t_walk, r_div, r_nodiv in active:
        t_walk = np.asarray(t_walk, float)
        arc  = np.array([_position_and_arc(t, t_waypoints_raw, W, H)[1] for t in t_walk])
        xy   = np.array([_path_xy_offset(s, W, H, delta) for s in arc])
        dist = np.linalg.norm(xy - rx, axis=1)
        series.append((label, dist, np.asarray(r_div, float), np.asarray(r_nodiv, float)))
        all_d.append(dist)
    all_d = np.concatenate(all_d) if all_d else np.array([0.0, 1.0])

    edges = _auto_distance_zones(all_d, n_zones)
    K = len(edges) - 1
    xpos = np.arange(K)
    zmask = lambda d, z: ((d >= edges[z]) & (d <= edges[z + 1])) if z == K - 1 \
        else ((d >= edges[z]) & (d < edges[z + 1]))
    def _zlabel(z):
        m = zmask(all_d, z)
        if np.any(m):
            return f'{all_d[m].min():.1f}–{all_d[m].max():.1f} m'
        return f'{edges[z]:.1f}–{edges[z + 1]:.1f} m'
    zlabels = [_zlabel(z) for z in range(K)]

    fig, ax = plt.subplots(figsize=(7.2, 4.6), constrained_layout=True)
    n_cfg = len(series)
    dodge = np.linspace(-0.24, 0.24, n_cfg) if n_cfg > 1 else np.array([0.0])

    for ci, (label, dist, r_div, r_nodiv) in enumerate(series):
        color = COND_COLORS.get(label)
        xs, dmed, dlo, dhi, nmed = [], [], [], [], []
        for z in range(K):
            m = zmask(dist, z)
            if not np.any(m):
                continue
            xs.append(xpos[z] + dodge[ci])
            dv, nv = r_div[m], r_nodiv[m]
            dmed.append(100 * np.nanmedian(dv))
            dlo.append(100 * np.nanpercentile(dv, 25))
            dhi.append(100 * np.nanpercentile(dv, 75))
            nmed.append(100 * np.nanmedian(nv))
        xs, dmed, nmed = np.array(xs), np.array(dmed), np.array(nmed)
        # vertical gain connectors (no-div → div)
        for x, dm, nm in zip(xs, dmed, nmed):
            ax.plot([x, x], [nm, dm], color=color, lw=1.0, alpha=0.45, zorder=2)
        # no-div: hollow markers + faint dashed line
        ax.plot(xs, nmed, linestyle=(0, (4, 2)), color=color, lw=1.1, alpha=0.6, zorder=3)
        ax.plot(xs, nmed, 'o', mfc='white', mec=color, mew=1.4, ms=6, zorder=3)
        # div: median ± IQR, filled marker, solid line
        yerr = np.vstack([dmed - np.array(dlo), np.array(dhi) - dmed])
        ax.errorbar(xs, dmed, yerr=yerr, fmt='-o', color=color, ecolor=color,
                    elinewidth=1.0, capsize=3, ms=7, lw=2.0,
                    markeredgecolor='white', markeredgewidth=0.6, zorder=4, label=label)

    ax.set_xticks(xpos)
    ax.set_xticklabels(zlabels)
    ax.set_xlabel('Distance from receiver (zone)')
    ax.set_ylabel('Reception rate  [%]')
    ax.set_ylim(0, 103)
    ax.set_xlim(-0.5, K - 0.5)
    ax.grid(True, axis='y', alpha=0.3)

    leg1 = ax.legend(title='Antenna config', loc='lower left', framealpha=0.9, fontsize=9)
    ax.add_artist(leg1)
    from matplotlib.lines import Line2D
    enc = [
        Line2D([0], [0], marker='o', color='gray', mfc='gray', mec='white',
               linestyle='-', label='time diversity (med ± IQR)'),
        Line2D([0], [0], marker='o', color='gray', mfc='white', mec='gray',
               linestyle=(0, (4, 2)), label='no diversity (median)'),
    ]
    ax.legend(handles=enc, loc='upper right', framealpha=0.9, fontsize=8,
              title='vertical gap = diversity gain')
    fig.suptitle('Reception rate vs distance from receiver', fontsize=11)
    return fig


def plot_colored_loop_all(configs, t_waypoints_raw, W, H, rx, delta, window_s):
    """
    configs: list of (label, t_walk, rate_timdiv_walk) — skip Nones.
    Produces one column per active config.
    """
    active = [(lbl, t, td) for lbl, t, td in configs if t is not None]
    n = len(active)

    cmap = PATH_CMAP
    norm = mcolors.Normalize(vmin=0, vmax=1)

    # Small-multiples: panels share the y-axis and pack tightly so the three
    # antenna configs sit side-by-side for direct comparison. Height is set by
    # the equal-aspect room, so keep the figure short to avoid empty whitespace.
    fig, axes = plt.subplots(1, n, figsize=(4.6 * n, 3.6), sharey=True,
                             constrained_layout=True)
    if n == 1:
        axes = [axes]

    for ax, (label, t_walk, rate_timdiv_walk) in zip(axes, active):
        print(f'  {label}: mapping {len(t_walk)} samples to path ...')
        _draw_room_path(ax, t_walk, rate_timdiv_walk, t_waypoints_raw,
                        W, H, rx, delta, label)

    # y-axis label and ticks only on the leftmost panel (shared axis).
    for j, ax in enumerate(axes):
        if j != 0:
            ax.set_ylabel('')
            ax.tick_params(labelleft=False)
    # Pull the panels close together.
    try:
        fig.get_layout_engine().set(w_pad=0.02, wspace=0.01)
    except Exception:
        pass

    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    fig.colorbar(sm, ax=axes, orientation='vertical', fraction=0.015, pad=0.01,
                 label='Time-diversity reception rate')
    # Single shared legend outside the axes, with explicit full-opacity handles
    # (the on-plot guide lines are faint, alpha 0.25). Lap entries use a triangle
    # in the lap colour to match the stemless direction arrows on the path.
    from matplotlib.lines import Line2D
    legend_handles = [
        Line2D([0], [0], marker='>', linestyle='None', markersize=9,
               markerfacecolor=CW_COLOR, markeredgecolor='white', label='Lap 1 CW'),
        Line2D([0], [0], marker='>', linestyle='None', markersize=9,
               markerfacecolor=CCW_COLOR, markeredgecolor='white', label='Lap 2 CCW'),
        Line2D([0], [0], marker='D', linestyle='None', markersize=10,
               markerfacecolor=START_COLOR, markeredgecolor='black', label='Start / End'),
        Line2D([0], [0], marker='*', linestyle='None', markersize=15,
               markerfacecolor=RX_COLOR, markeredgecolor='white', label='Rx'),
    ]
    fig.legend(handles=legend_handles, loc='outside lower center',
               ncol=len(legend_handles), fontsize=8)
    fig.suptitle(
        f'Walk path colored by time-diversity reception rate  '
        f'(W = {WINDOW_SAMPLES} samples ≈ {window_s:.2f} s)',
        fontsize=11)
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    mat_paths = [MAT_ANT1, MAT_ANT2, MAT_BOTH]

    results = []   # (label, t_axis, rate_nodiv, rate_timdiv, duration_s, output_rate)
    first_duration = None
    for label, mat_path in zip(LABELS, mat_paths):
        if mat_path is None:
            results.append((label, None, None, None, None, None))
            continue
        print(f'\nLoading and decoding [{label}]  {mat_path} ...')
        try:
            quality, output_rate = decode_full_file(mat_path)
        except Exception as exc:
            print(f'  FAILED: {exc}')
            results.append((label, None, None, None, None, None))
            continue
        save_quality_timeseries(mat_path, quality, output_rate, video_path=WALK_VIDEO)
        N          = len(quality)
        duration_s = N / output_rate
        if first_duration is None:
            first_duration = duration_s
        window_s = WINDOW_SAMPLES / output_rate
        print(f'  {N} samples  |  {output_rate:.1f} Hz  |  {duration_s:.1f} s  '
              f'|  window {window_s:.3f} s')

        rate_nodiv, rate_timdiv = sliding_rates(quality, WINDOW_SAMPLES)
        t_axis = np.arange(N) / output_rate
        results.append((label, t_axis, rate_nodiv, rate_timdiv, duration_s, output_rate))

    # Use the longest active recording to set walk_end if not specified
    active_durations = [r[4] for r in results if r[4] is not None]
    walk_end = WALK_END_S if WALK_END_S is not None else (
        max(active_durations) if active_durations else 0.0)
    t_waypoints_raw = ([WALK_START_S] + list(WALK_CORNER_TIMES_7) + [walk_end])

    # Print per-config walk-window means
    print('\n── Walk-window means ──')
    for label, t_axis, rate_nodiv, rate_timdiv, *_ in results:
        if t_axis is None:
            print(f'  {label:20s}  —')
            continue
        walk_mask = (t_axis >= WALK_START_S) & (t_axis <= walk_end)
        if walk_mask.any():
            m_td = 100 * float(np.nanmean(rate_timdiv[walk_mask]))
            m_nd = 100 * float(np.nanmean(rate_nodiv[walk_mask]))
            print(f'  {label:20s}  time-div {m_td:.1f}%   no-div {m_nd:.1f}%')

    # Figure 1 — rate vs time
    fig1_configs = [(lbl, t, nd, td)
                    for lbl, t, nd, td, *_ in results]
    window_s_display = (WINDOW_SAMPLES / results[next(
        i for i, r in enumerate(results) if r[1] is not None)][5])
    print('\nPlotting Figure 1 (rate vs time) ...')
    plot_rate_vs_time_all(fig1_configs, WALK_START_S, walk_end, window_s_display)

    # Figure 2 — colored path
    fig2_configs = []
    for label, t_axis, _, rate_timdiv, *__ in results:
        if t_axis is None:
            fig2_configs.append((label, None, None))
            continue
        walk_mask = (t_axis >= WALK_START_S) & (t_axis <= walk_end)
        fig2_configs.append((label, t_axis[walk_mask], rate_timdiv[walk_mask]))
    print('Plotting Figure 2 (colored paths) ...')
    plot_colored_loop_all(fig2_configs, t_waypoints_raw,
                          W_M, H_M, RX_M, DELTA_PATH, window_s_display)

    # Figure 3 — reception rate vs distance from Rx (zone-aggregated, div + no-div)
    fig3_configs = []
    for label, t_axis, rate_nodiv, rate_timdiv, *__ in results:
        if t_axis is None:
            fig3_configs.append((label, None, None, None))
            continue
        walk_mask = (t_axis >= WALK_START_S) & (t_axis <= walk_end)
        fig3_configs.append((label, t_axis[walk_mask],
                             rate_timdiv[walk_mask], rate_nodiv[walk_mask]))
    print('Plotting Figure 3 (rate vs distance) ...')
    plot_rate_vs_distance(fig3_configs, t_waypoints_raw,
                          W_M, H_M, RX_M, DELTA_PATH)

    _save_all_figures(PLOTS_DIR)
    plt.show()


if __name__ == '__main__':
    main()
