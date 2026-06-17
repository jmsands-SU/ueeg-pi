#!/usr/bin/env python3
"""
survey_diversity_plot.py

Post-processes the output of run_snapshot_survey.m to compare antenna
diversity and time diversity across the full recording.

Reads:
  snapshot_survey_results.mat       — push1 rates + timing (alongside this script)
  /mnt/c/Temp/snapshots/            — per-snapshot sim mat files for TSBR decode

Produces:
  Figure 1 — reception rate vs recording time (one line per combination)
  Figure 2 — bar chart: mean ± std across all snapshots
              (3 antenna configs × push1 / no-time-div / with-time-div)

Usage (WSL):
    python survey_diversity_plot.py
"""

import os
import sys
import glob
import numpy as np
import scipy.io
import matplotlib.pyplot as plt

# ── Paths ──────────────────────────────────────────────────────────────────────
_DIR         = os.path.dirname(os.path.abspath(__file__))
SURVEY_MAT   = os.path.join(_DIR, 'snapshot_survey_results_lowerpacketthreshold.mat')
SNAPSHOT_DIR = '/mnt/c/Temp/snapshots_lowerpacketthreshold'
GCS_CHANNEL  = '2'
VIDEO_PATH   = '/mnt/g/Shared drives/Spontaneous_EEG_paper/Data/characterization/wireless_link/0601_longloopvideo.MOV'
# '/mnt/g/Shared drives/Spontaneous_EEG_paper/Data/characterization/wireless_link/IMG_8019.MOV'
ENABLE_FILMSTRIP = True   # set True to extract video frames (slow over network share)
PLOTS_DIR    = os.path.join(_DIR, 'plots')   # every figure is saved here for review

# ── Room walk configuration ────────────────────────────────────────────────────
# Times (seconds in recording) when the participant reached each corner.
# Walk starts at snapshot_times[0] (CW lap) and ends at snapshot_times[-1].
# Path order: BR→BL→TL→TR→BR  (CW), then BR→TR→TL→BL→BR  (CCW).
# Set to None for any unknown corner → uniform-speed interpolation is used.
WALK_CORNER_TIMES = [37, 46, 58, 64, 71, 82, 90]  # 7 intermediates
# [37, 46, 58, 64, 71, 82, 90]  # 7 intermediates
# ── TimeStampBasedReader import ────────────────────────────────────────────────
sys.path.insert(0, _DIR)
HAS_TSBR = False
try:
    from sdr_reader_gcs_write import TimeStampBasedReader
    HAS_TSBR = True
except ImportError:
    print('WARNING: TimeStampBasedReader not importable.\n'
          '  sdr_reader_gcs_write.py must be in the same directory as this script.\n'
          '  Time-diversity lines and bars will be skipped.\n')


# ══════════════════════════════════════════════════════════════════════════════
# _load_mat  — handles v5 (scipy.io) and v7.3 HDF5 (h5py)
# ══════════════════════════════════════════════════════════════════════════════
def _load_mat(filepath, varnames):
    try:
        d = scipy.io.loadmat(filepath, squeeze_me=True,
                             variable_names=list(varnames))
        return {k: d[k] for k in varnames if k in d}
    except NotImplementedError:
        pass
    except Exception as exc:
        raise RuntimeError(f'scipy.io.loadmat failed: {exc}') from exc

    try:
        import h5py
    except ImportError as exc:
        raise RuntimeError(
            'Mat file is MATLAB v7.3 (HDF5) but h5py is not installed.\n'
            'Install with:  pip install h5py') from exc

    result = {}
    with h5py.File(filepath, 'r') as f:
        for k in varnames:
            if k not in f:
                continue
            raw = f[k][()]
            if raw.ndim == 0 or raw.size == 1:
                result[k] = float(raw.ravel()[0])
            else:
                result[k] = raw.ravel().astype(np.float64)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# decode_snapshot  — TSBR decode for one mat file
# ══════════════════════════════════════════════════════════════════════════════
def decode_snapshot(mat_path, snap_dur):
    """
    Returns (rate_nodiv, rate_div, q_counts) for one snapshot mat file,
    or (nan, nan, {}).
      rate_nodiv = avg single-copy baseline  = 1 - (v1+v2+2×neither)/(2N)
      rate_div   = with-time-diversity rate  = both_recv / N
      q_counts   = dict mapping raw quality byte value → count
    """
    reader = TimeStampBasedReader(enable_gcs=False, gcs_channels=(1, 2, 3, 4),accepted_frame_lengths=(250, 248),frame_length_counts={250: 18, 248: 1})
    expected_N = 0
    try:
        d = _load_mat(mat_path, ['push1', 'databit1', 'changeofstrength1', 'packetnum1'])
        push      = np.asarray(d['push1'],                                           dtype=np.uint16).ravel()
        N0        = len(push)
        # expected_N from recording duration, independent of signal quality
        expected_N = round(snap_dur * reader.output_rate_hz)
        # recv_N: push rising-edges / 160 — actual received time slots (diagnostic column)
        recv_N = int(np.sum(np.diff(push.astype(int)) > 0)) // 160
        databit   = np.asarray(d.get('databit1',          np.zeros(N0, np.uint16)), dtype=np.uint16).ravel()
        error     = np.asarray(d.get('changeofstrength1', np.zeros(N0, np.uint16)), dtype=np.uint16).ravel()
        packetnum = np.asarray(d.get('packetnum1',        np.zeros(N0, np.uint16)), dtype=np.uint16).ravel()

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

        words = ((databit & 0x1)
                 | ((packetnum & 0x7) << 4)
                 | ((error    & 0x1)  << 7)
                 | ((push     & 0x1)  << 8)).astype(np.uint16)

        reader.decode_from_word_stream(words)
        _values, quality_raw = reader.get_decoded_arrays(GCS_CHANNEL)
    except Exception as exc:
        print(f'    TSBR failed ({os.path.basename(mat_path)}): {exc}')
        return np.nan, np.nan, {}, 0

    quality_raw = np.asarray(quality_raw)
    quality = quality_raw.ravel().astype(int)
    q_base  = quality & 0x07
    N = len(q_base)

    # Normalize N so all snapshots of the same duration share the same denominator.
    # TSBR fills internal gaps with q=0; remaining variation comes from the leading
    # edge (words before first frame sync) and trailing incomplete groups. Pad with
    # q=0 (neither) so that the denominator always reflects total available time slots.
    # NOTE: do this before the N==0 check so complete-loss snapshots get rate=0, not nan.
    if expected_N > 0 and N != expected_N:
        if N < expected_N:
            quality = np.concatenate([quality, np.zeros(expected_N - N, dtype=int)])
        else:
            quality = quality[:expected_N]
        q_base = quality & 0x07
        N = expected_N

    if N == 0:
        return np.nan, np.nan, {}, recv_N

    only_v1   = q_base == 1
    only_v2   = q_base == 2
    both_recv = q_base == 3
    neither   = q_base == 0

    unique, counts = np.unique(quality, return_counts=True)
    q_counts = {int(k): int(v) for k, v in zip(unique, counts)}

    rate_div   = 1.0 - float(neither.mean())
    loss_nodiv = (int(only_v1.sum()) + int(only_v2.sum())
                  + 2 * int(neither.sum())) / (2 * N)
    return 1.0 - loss_nodiv, rate_div, q_counts, recv_N


# ══════════════════════════════════════════════════════════════════════════════
# extract_video_frames  — grab one frame per snapshot from participant video
# ══════════════════════════════════════════════════════════════════════════════
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


def extract_video_frames(video_path, times_s):
    """
    Returns a list of RGB numpy arrays, one per entry in times_s.
    Missing/unreadable frames are replaced with a grey placeholder.
    """
    try:
        import cv2
    except ImportError:
        print('WARNING: opencv-python-headless not installed — filmstrip skipped.\n'
              '  Install with: pip install opencv-python-headless')
        return None

    if not os.path.exists(video_path):
        print(f'WARNING: video not found: {video_path}\n  Filmstrip skipped.')
        return None

    cap = cv2.VideoCapture(video_path)
    frames = []
    for t in times_s:
        cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000.0)   # ms-based seek uses index, not decode
        ok, frame = cap.read()
        if ok:
            frames.append(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        else:
            # grey placeholder
            frames.append(np.full((240, 320, 3), 180, dtype=np.uint8))
    cap.release()
    return frames


def build_filmstrip_figure(frames, snapshot_times, nodiv_rates_1d, timdiv_rates_1d):
    """
    Figure 3: grid of video thumbnails for the 'both antennas' config.
    Border colour = time-diversity reception rate (red→green).
    """
    import math
    n = len(snapshot_times)
    ncols = math.ceil(math.sqrt(n))
    nrows = math.ceil(n / ncols)

    fig, axes = plt.subplots(nrows, ncols,
                             figsize=(ncols * 3.2, nrows * 2.6),
                             constrained_layout=True)
    axes_flat = np.asarray(axes).ravel()

    cmap_rate = plt.cm.RdYlGn

    for s in range(n):
        ax = axes_flat[s]
        ax.imshow(frames[s])
        ax.set_xticks([])
        ax.set_yticks([])

        rate = timdiv_rates_1d[s] if not np.isnan(timdiv_rates_1d[s]) else 0.0
        colour = cmap_rate(rate)
        for spine in ax.spines.values():
            spine.set_edgecolor(colour)
            spine.set_linewidth(4)

        ax.set_title(f't={int(snapshot_times[s])} s', fontsize=9, pad=3)

        if not np.isnan(timdiv_rates_1d[s]):
            td = timdiv_rates_1d[s]
            nd = nodiv_rates_1d[s]
            label_txt = (f'{100*td:.0f}%' if np.isnan(nd)
                         else f'{100*td:.0f}% (↑{100*(td-nd):.0f}pp)')
        else:
            label_txt = '—'
        ax.set_xlabel(label_txt, fontsize=8, labelpad=3)

    # hide unused cells
    for s in range(n, len(axes_flat)):
        axes_flat[s].set_visible(False)

    fig.suptitle(
        'Participant video — both antennas  '
        '(border colour = time-diversity reception rate)',
        fontsize=10,
    )

    sm = plt.cm.ScalarMappable(cmap=cmap_rate, norm=plt.Normalize(vmin=0, vmax=1))
    sm.set_array([])
    fig.colorbar(sm, ax=axes_flat[:n], orientation='vertical',
                 fraction=0.015, pad=0.01, label='Time-div rate')
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# Room walk diagram helpers (Figure 4)
# ══════════════════════════════════════════════════════════════════════════════

def _perimeter_arc_anchors(W, H):
    """Cumulative arc-distances (m) for the 9 path waypoints of the 2-lap walk.
    CW lap: BR→BL→TL→TR→BR, then CCW: BR→TR→TL→BL→BR."""
    return np.array([0, W, W+H, 2*W+H, 2*(W+H),
                     2*W+3*H, 3*W+3*H, 3*W+4*H, 4*(W+H)])


def _perimeter_xy(s, W, H):
    """Map arc-distance s (m) along the 2-lap path to room (x, y) in metres."""
    H, W, s = float(H), float(W), float(s)
    if s <= W:                  return (W - s, 0.0)
    elif s <= W + H:            return (0.0, s - W)
    elif s <= 2*W + H:          return (s - W - H, H)
    elif s <= 2*(W + H):        return (W, H - (s - 2*W - H))
    elif s <= 2*W + 3*H:        return (W, s - 2*(W + H))
    elif s <= 3*W + 3*H:        return (W - (s - 2*W - 3*H), H)
    elif s <= 3*W + 4*H:        return (0.0, H - (s - 3*W - 3*H))
    else:                       return (s - 3*W - 4*H, 0.0)


def _fill_corner_times(t_waypoints_raw, W, H):
    """Fill None entries in the 9-element waypoint-time list using
    arc-proportional linear interpolation between known anchors."""
    arcs = _perimeter_arc_anchors(W, H)
    t_wpts = list(t_waypoints_raw)
    known = [(i, float(t_wpts[i])) for i in range(len(t_wpts))
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
    """Return ((x, y), arc_s) for recording time t along the 2-lap walk."""
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
    """Like _perimeter_xy but shifted onto the actual drawn path line.
    W×H are the given path dimensions (outer CCW loop).
    Lap 1 CW: delta inside the W×H boundary.
    Lap 2 CCW: exactly at the W×H boundary."""
    H, W, s, delta = float(H), float(W), float(s), float(delta)
    perimeter = 2 * (W + H)
    # Lap 1 CW — inner path (exact corners at delta inside W×H)
    if s <= W:                  return (W - delta - s * (W - 2*delta) / W,            delta)
    elif s <= W + H:            return (delta,                                          delta + (s - W) * (H - 2*delta) / H)
    elif s <= 2*W + H:          return (delta + (s - W - H) * (W - 2*delta) / W,      H - delta)
    elif s <= perimeter:        return (W - delta,                                      H - delta - (s - 2*W - H) * (H - 2*delta) / H)
    # Lap 2 CCW — outer path at exactly W×H (no offset)
    elif s <= 2*W + 3*H:        return (W,                   s - perimeter)
    elif s <= 3*W + 3*H:        return (W - (s - 2*W - 3*H), H)
    elif s <= 3*W + 4*H:        return (0.0,                  H - (s - 3*W - 3*H))
    else:                       return (s - 3*W - 4*H,        0.0)


def _add_path_arrow(ax, p0, p1, color, frac=0.4):
    """Draw a direction arrow at position `frac` along p0→p1."""
    p0, p1 = np.asarray(p0, float), np.asarray(p1, float)
    mid  = p0 + frac * (p1 - p0)
    step = 0.35 * (p1 - p0) / max(np.linalg.norm(p1 - p0), 1e-6)
    ax.annotate('', xy=mid + step, xytext=mid - step,
                arrowprops=dict(arrowstyle='->', color=color, lw=1.8), zorder=4)


def plot_walk_diagram(snapshot_times, W, H, rx, t_waypoints_raw, rates=None):
    """Figure 4: room diagram with snapshot positions and receiver distances."""
    delta = 0.55          # visual separation between the two path lines (m)
    perimeter = 2 * (W + H)
    arcs = _perimeter_arc_anchors(W, H)
    seg_labels = ['bot←', 'left↑', 'top→', 'right↓',
                  'right↑', 'top←', 'left↓', 'bot→']

    # ── Compute positions for all snapshots ──────────────────────────────────
    n = len(snapshot_times)
    positions, dot_positions, arc_ss, distances = [], [], [], []
    for t in snapshot_times:
        xy, arc_s = _position_and_arc(t, t_waypoints_raw, W, H)
        positions.append(xy)
        dot_positions.append(_path_xy_offset(arc_s, W, H, delta))
        arc_ss.append(arc_s)
        distances.append(np.hypot(xy[0] - rx[0], xy[1] - rx[1]))

    # ── Figure layout ─────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(15, 6.5))
    fig.suptitle('Room walk — snapshot positions & distance to receiver',
                 fontsize=12, fontweight='bold')

    ax = fig.add_axes([0.03, 0.08, 0.57, 0.84])
    ax.set_aspect('equal')
    ax.set_xlabel('x  [m]', fontsize=9)
    ax.set_ylabel('y  [m]', fontsize=9)
    ax.set_title(f'Path {W:.2f} m × {H:.2f} m   |   Rx at ({rx[0]:.2f}, {rx[1]:.2f}) m',
                 fontsize=9)

    # CW lap: inner path — BR→BL→TL→TR→BR (delta inside W×H)
    cw_path = np.array([
        [W - delta,  delta],
        [delta,      delta],
        [delta,     H - delta],
        [W - delta, H - delta],
        [W - delta,  delta],
    ])
    ax.plot(cw_path[:, 0], cw_path[:, 1], '-', color='steelblue',
            linewidth=1.8, alpha=0.75, label='Lap 1 (CW)', zorder=2)
    for i in range(4):
        _add_path_arrow(ax, cw_path[i], cw_path[i + 1], 'steelblue', frac=0.35)

    # CCW lap: outer path — BR→TR→TL→BL→BR (at W×H boundary)
    ccw_path = np.array([
        [W,   0.0],
        [W,   H],
        [0.0, H],
        [0.0, 0.0],
        [W,   0.0],
    ])
    ax.plot(ccw_path[:, 0], ccw_path[:, 1], '--', color='darkorange',
            linewidth=1.8, alpha=0.75, label='Lap 2 (CCW)', zorder=2)
    for i in range(4):
        _add_path_arrow(ax, ccw_path[i], ccw_path[i + 1], 'darkorange', frac=0.35)

    # Start/end marker
    br = _perimeter_xy(0, W, H)
    ax.plot(br[0], br[1], 'D', markersize=12, color='limegreen',
            markeredgecolor='white', markeredgewidth=0.8, zorder=6,
            label='Start / End (BR)')

    # Receiver
    ax.plot(rx[0], rx[1], '*', markersize=22, color='crimson',
            markeredgecolor='white', markeredgewidth=0.7, zorder=6, label='Receiver')
    ax.text(rx[0] + 0.15, rx[1] + 0.35, 'Rx', fontsize=9,
            fontweight='bold', color='crimson')

    # Snapshot markers — coloured by reception rate (RdYlGn) or snap index if no rates
    cmap_rate = plt.cm.RdYlGn
    norm_rate  = plt.Normalize(vmin=0, vmax=1)
    for s_idx, (xy, dxy) in enumerate(zip(positions, dot_positions)):
        if rates is not None and not np.isnan(rates[s_idx]):
            color = cmap_rate(norm_rate(rates[s_idx]))
        else:
            color = plt.cm.plasma(s_idx / max(n - 1, 1))
        ax.plot([dxy[0], rx[0]], [dxy[1], rx[1]], '-',
                color='#cccccc', linewidth=0.7, zorder=1)
        ax.plot(dxy[0], dxy[1], 'o', markersize=22, color=color,
                markeredgecolor='white', markeredgewidth=0.8, zorder=5)
        r_c, g_c, b_c = color[0], color[1], color[2]
        lum = 0.299 * r_c + 0.587 * g_c + 0.114 * b_c
        txt_color = 'black' if lum > 0.55 else 'white'
        ax.text(dxy[0], dxy[1], str(s_idx + 1), ha='center', va='center',
                fontsize=7.5, fontweight='bold', color=txt_color, zorder=7)

    ax.set_xlim(-0.4, W + 0.4)
    ax.set_ylim(-0.4, H + 0.4)
    ax.legend(loc='upper right', fontsize=8, framealpha=0.9)
    ax.tick_params(labelsize=8)

    if rates is not None:
        sm = plt.cm.ScalarMappable(cmap=cmap_rate, norm=norm_rate)
        sm.set_array([])
        fig.colorbar(sm, ax=ax, orientation='vertical', fraction=0.03, pad=0.02,
                     label='Time-div reception rate (both antennas)')

    # ── Right panel: distance table ───────────────────────────────────────────
    ax_t = fig.add_axes([0.62, 0.04, 0.37, 0.90])
    ax_t.axis('off')

    has_rates = rates is not None
    header  = (f'{"#":>3}  {"t(s)":>4}  {"Lap":>3}  {"Edge":>6}  {"d (m)":>6}'
               + (f'  {"rate":>5}' if has_rates else ''))
    divider = '─' * len(header)
    rows = [header, divider]
    for s_idx in range(n):
        arc_s = arc_ss[s_idx]
        lap = 1 if arc_s < perimeter - 1e-9 else 2
        seg = max((k for k in range(len(arcs) - 1)
                   if arcs[k] <= arc_s + 1e-9), default=0)
        rate_str = ''
        if has_rates:
            r = rates[s_idx]
            rate_str = f'  {100*r:>4.0f}%' if not np.isnan(r) else '     —'
        rows.append(
            f'{s_idx + 1:>3}  {int(snapshot_times[s_idx]):>4}  '
            f'{lap:>3}  {seg_labels[seg]:>6}  {distances[s_idx]:>6.2f}{rate_str}')

    ax_t.text(0.02, 0.97, '\n'.join(rows), transform=ax_t.transAxes,
              fontsize=9, family='monospace', verticalalignment='top')
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# main
# ══════════════════════════════════════════════════════════════════════════════
def main():
    # ── Load survey summary ────────────────────────────────────────────────────
    print(f'Loading {SURVEY_MAT} ...')
    try:
        d = _load_mat(SURVEY_MAT,
                      ['snapshot_times', 'SNAPSHOT_DUR', 'completed'])
    except Exception as exc:
        print(f'Cannot load survey results: {exc}')
        sys.exit(1)

    snapshot_times = np.asarray(d['snapshot_times']).ravel()
    snap_dur       = float(d.get('SNAPSHOT_DUR', .5))
    n_completed    = int(d.get('completed', len(snapshot_times)))

    # Trim to completed snapshots only (in case run was interrupted)
    snapshot_times = snapshot_times[:n_completed]
    nSnap          = n_completed

    print(f'  {nSnap} snapshots  |  '
          f't = {snapshot_times[0]:.0f} – {snapshot_times[-1]:.0f} s  |  '
          f'{snap_dur:.1f} s each')

    labels   = ['Ant 1 only', 'Ant 2 only', 'Both antennas']
    prefixes = ['ant1only',   'ant2only',   'both']

    # ── TSBR decode ───────────────────────────────────────────────────────────
    # nodiv_rates / timdiv_rates: shape (nSnap, 3)
    nodiv_rates  = np.full((nSnap, 3), np.nan)
    timdiv_rates = np.full((nSnap, 3), np.nan)
    recv_n_all   = np.full((nSnap, 3), -1, dtype=int)   # push rising-edges / 160
    # q_counts_all[s][r] = dict of quality_byte → count
    q_counts_all = [[{} for _ in range(3)] for _ in range(nSnap)]

    if HAS_TSBR:
        print(f'\nTSBR decode from {SNAPSHOT_DIR} ...')
        for s in range(nSnap):
            idx = s + 1    # MATLAB wrote 1-indexed filenames
            print(f'  Snapshot {idx}/{nSnap}  (t = {snapshot_times[s]:.0f} s)')
            for r, prefix in enumerate(prefixes):
                path = os.path.join(SNAPSHOT_DIR, f'{prefix}_s{idx:02d}.mat')
                if not os.path.exists(path):
                    print(f'    {os.path.basename(path)} not found — skipping')
                    continue
                nd, td, qc, rn = decode_snapshot(path, snap_dur)
                nodiv_rates[s, r]  = nd
                timdiv_rates[s, r] = td
                q_counts_all[s][r] = qc
                recv_n_all[s, r]   = rn
        print()

    # ── Quality table ──────────────────────────────────────────────────────────
    if HAS_TSBR:
        # Collect all quality codes seen across all snapshots/configs
        all_q_codes = sorted({q for row in q_counts_all for d in row for q in d})
        # Human-readable label for each code
        _base_names = {0: 'neither', 1: 'v1_only', 2: 'v2_only', 3: 'both',
                       5: 'mis→v1', 6: 'mis→v2'}
        def _q_label(q):
            base = q & 0x07
            err  = bool(q & 0x08)
            name = _base_names.get(base, f'q{base}')
            return f'{name}+e' if err else name

        q_labels = [_q_label(q) for q in all_q_codes]

        for r, label in enumerate(labels):
            print(f'\n── {label} ──')
            # header
            hdr = f'{"Snap":>4}  {"t(s)":>5}  {"N":>5}  {"Rate-div":>9}  {"Rate-nodiv":>10}'
            for ql in q_labels:
                hdr += f'  {ql:>9}'
            print(hdr)
            print('─' * len(hdr))
            for s in range(nSnap):
                qc = q_counts_all[s][r]
                if not qc:
                    continue
                N_s = sum(qc.values())
                td  = timdiv_rates[s, r]
                nd  = nodiv_rates[s, r]
                row = (f'{s+1:>4}  {int(snapshot_times[s]):>5}  {N_s:>5}'
                       f'  {100*td:>8.1f}%  {100*nd:>9.1f}%')
                for q in all_q_codes:
                    cnt = qc.get(q, 0)
                    row += f'  {cnt:>9}'
                print(row)
        print()

    # ══════════════════════════════════════════════════════════════════════════
    # Figure 1 — reception rate vs recording time
    # ══════════════════════════════════════════════════════════════════════════
    ant_colors = ['#3366cc', '#cc3333', '#22aa44']

    # Average of the two single-antenna configs — antenna-diversity baseline
    avg_nodiv_single  = np.nanmean(nodiv_rates[:,  :2], axis=1)
    avg_timdiv_single = np.nanmean(timdiv_rates[:, :2], axis=1)

    fig1, axes = plt.subplots(3, 1, figsize=(12, 8), sharex=True,
                              constrained_layout=True)
    for r in range(3):
        ax = axes[r]
        tsbr_ok = HAS_TSBR and not np.all(np.isnan(nodiv_rates[:, r]))
        if tsbr_ok:
            ax.plot(snapshot_times, 100 * nodiv_rates[:, r],
                    's--', color='#e07820', linewidth=1.2, markersize=4,
                    label='No time diversity  (avg single-copy)')
            ax.plot(snapshot_times, 100 * timdiv_rates[:, r],
                    '^-', color='#555555', linewidth=1.2, markersize=4,
                    label='With time diversity')
            # On the "Both antennas" subplot add the single-antenna average for comparison
            if r == 2:
                ax.plot(snapshot_times, 100 * avg_nodiv_single,
                        's:', color='#e07820', linewidth=1.2, markersize=4, alpha=0.5,
                        label='Single-ant avg, no time div')
                ax.plot(snapshot_times, 100 * avg_timdiv_single,
                        '^:', color='#555555', linewidth=1.2, markersize=4, alpha=0.5,
                        label='Single-ant avg, with time div')
        else:
            ax.text(0.5, 0.5, 'No TSBR data',
                    ha='center', va='center', transform=ax.transAxes)

        ax.set_ylim(0, 105)
        ax.set_ylabel('Reception rate (%)')
        ax.set_title(labels[r])
        ax.legend(loc='lower right', fontsize=8)
        ax.grid(True, alpha=0.4)

    axes[-1].set_xlabel('Time into recording (s)')
    fig1.suptitle(
        f'Packet reception vs recording position  '
        f'({nSnap} snapshots, {snap_dur:.1f} s each)',
        fontsize=11)

    # ══════════════════════════════════════════════════════════════════════════
    # Figure 2 — aggregated bar chart: mean ± std across all snapshots
    # ══════════════════════════════════════════════════════════════════════════
    def _ms(arr):
        """Return (mean%, std%) ignoring NaN."""
        v = arr[~np.isnan(arr)]
        return (100 * v.mean() if len(v) else 0.0,
                100 * v.std()  if len(v) else 0.0)

    tsbr_any = HAS_TSBR and not np.all(np.isnan(nodiv_rates))

    fig2, ax2 = plt.subplots(figsize=(10, 5))

    if tsbr_any:
        nd_m, nd_s = zip(*[_ms(nodiv_rates[:, r])  for r in range(3)])
        td_m, td_s = zip(*[_ms(timdiv_rates[:, r]) for r in range(3)])
        x     = np.arange(3)
        width = 0.35
        kw    = dict(capsize=5, alpha=0.85, zorder=3)

        b2 = ax2.bar(x - width / 2, nd_m, width,
                     yerr=nd_s,
                     label='No time diversity  (avg single-copy baseline)',
                     color='#e07820', **kw)
        b3 = ax2.bar(x + width / 2, td_m, width,
                     yerr=td_s,
                     label='With time diversity',
                     color='#22aa44', **kw)

        for bars, means in [(b2, nd_m), (b3, td_m)]:
            for b, m in zip(bars, means):
                ax2.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.5,
                         f'{m:.0f}%', ha='center', va='bottom', fontsize=8)

    ax2.set_xticks(x)
    ax2.set_xticklabels(labels)
    ax2.set_ylabel('Mean packet reception rate (%)')
    ax2.set_ylim(0, 118)
    ax2.set_title(
        f'Antenna & time diversity — mean ± std  '
        f'({nSnap} snapshots, '
        f't = {snapshot_times[0]:.0f}–{snapshot_times[-1]:.0f} s, '
        f'{snap_dur:.1f} s each)')
    ax2.legend(loc='lower right', fontsize=9)
    ax2.grid(axis='y', alpha=0.35, zorder=0)
    fig2.tight_layout()

    # ── Print summary table ────────────────────────────────────────────────────
    print('\n══ Summary ══')
    if tsbr_any:
        print(f'{"":20s}  {"No-div":>12s}  {"Time-div":>12s}')
        for r in range(3):
            print(f'{labels[r]:20s}  {nd_m[r]:6.1f}±{nd_s[r]:.1f}%'
                  f'  {td_m[r]:6.1f}±{td_s[r]:.1f}%')

    # ══════════════════════════════════════════════════════════════════════════
    # Figure 3 — participant video filmstrip
    # ══════════════════════════════════════════════════════════════════════════
    # Extract frame at the midpoint of each snapshot window
    frames = None
    if ENABLE_FILMSTRIP:
        frame_times = (snapshot_times - snapshot_times[0]) + snap_dur / 2.0
        frames = extract_video_frames(VIDEO_PATH, frame_times)
    if frames is not None:
        both_idx = labels.index('Both antennas')
        build_filmstrip_figure(frames, snapshot_times,
                               nodiv_rates[:, both_idx],
                               timdiv_rates[:, both_idx])

    # ══════════════════════════════════════════════════════════════════════════
    # Figure 4 — room walk diagram
    # ══════════════════════════════════════════════════════════════════════════
    W_m  = 48 * 0.3048
    H_m  = 28 * 0.3048
    rx_m = (W_m / 2, 3 * 0.3048)
    t_wpts_raw = ([float(snapshot_times[0])]
                  + WALK_CORNER_TIMES
                  + [float(snapshot_times[-1])])
    both_idx = labels.index('Both antennas')
    walk_rates = timdiv_rates[:, both_idx] if HAS_TSBR else None
    plot_walk_diagram(snapshot_times, W_m, H_m, rx_m, t_wpts_raw, rates=walk_rates)

    _save_all_figures(PLOTS_DIR)
    plt.show()


if __name__ == '__main__':
    main()
