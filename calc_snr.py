"""
calc_snr.py — compute SNR of an injected sine tone from a time-domain channel CSV.

Usage:
    python3 calc_snr.py --freq 10.0
    python3 calc_snr.py --freq 10.0 --ch 1 --csv-dir /path/to/csvs
    python3 calc_snr.py --freq 10.0 --file /path/to/file.csv
    python3 calc_snr.py --freq 10.0 --band 0.5 50   # restrict SNR to 0.5–50 Hz band

The CSV format is:
    time_s, amplitude, quality_code
(as produced by the sdr_reader_gcs_write pipeline)

Method:
    1. Load amplitude, optionally filtering to quality codes 1/2/3/5/6 (valid packets).
    2. FFT the signal.
    3. Restrict to --band [flo, fhi] (default 0.5–50 Hz); bins outside this range are ignored.
    4. Signal power  = sum of in-band FFT bins within ±bw Hz of the target frequency.
    5. Noise power   = remaining in-band power (excluding signal bins).
    6. SNR (dB)      = 10 * log10(signal_power / noise_power)
"""

import argparse
import numpy as np
import pandas as pd


DEFAULT_CSV_DIR = '/home/joannas/bladeRF/hdl/pythonscripts'
DEFAULT_CHANNEL = 3
SIGNAL_BW_HZ = 2.0   # ± Hz around target frequency counted as signal
DEFAULT_BAND = (0.5, 50.0)  # Hz — in-band SNR window


def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, comment=None)
    # Header contains a pipe-delimited description; trim column name to first token
    df.columns = [c.split('|')[0].strip() for c in df.columns]
    return df


def calc_snr(amplitude: np.ndarray, fs: float, tone_freq: float,
             bw_hz: float = SIGNAL_BW_HZ,
             band: tuple = DEFAULT_BAND) -> dict:
    """
    Compute SNR within `band` (flo, fhi) Hz only.
    Signal bins: within ±bw_hz of tone_freq AND inside band.
    Noise bins:  inside band but outside signal bins.
    """
    n = len(amplitude)
    # Remove DC before FFT to avoid DC leakage swamping noise estimate
    amplitude = amplitude - amplitude.mean()

    fft_vals = np.fft.rfft(amplitude)
    freqs = np.fft.rfftfreq(n, d=1.0 / fs)

    # One-sided power spectrum normalised so that sum(fft_power) == mean(amplitude²)
    # (Parseval for rfft: double all bins except DC and Nyquist, divide by n²)
    fft_power = (np.abs(fft_vals) ** 2) / (n ** 2)
    fft_power[1:-1] *= 2

    freq_res = freqs[1] - freqs[0]

    flo, fhi = band
    band_mask   = (freqs >= flo) & (freqs <= fhi)
    signal_mask = band_mask & (np.abs(freqs - tone_freq) <= bw_hz)
    noise_mask  = band_mask & ~signal_mask

    signal_power = fft_power[signal_mask].sum()
    noise_power  = fft_power[noise_mask].sum()

    snr_db = 10.0 * np.log10(signal_power / noise_power) if noise_power > 0 else float('inf')

    # Peak within the analysis band only
    band_power = np.where(band_mask, fft_power, 0.0)
    peak_freq = freqs[np.argmax(band_power)]

    return {
        'snr_db': snr_db,
        'signal_power': signal_power,
        'noise_power': noise_power,
        'fft_freq_resolution_hz': freq_res,
        'n_samples': n,
        'fs_hz': fs,
        'tone_freq_hz': tone_freq,
        'signal_bw_hz': bw_hz,
        'band_hz': band,
        'peak_freq_hz': peak_freq,
    }


def main():
    parser = argparse.ArgumentParser(description='Compute SNR of an injected sine tone.')
    parser.add_argument('--freq', type=float, required=True,
                        help='Injected tone frequency in Hz')
    parser.add_argument('--ch', type=int, default=DEFAULT_CHANNEL,
                        help=f'Channel index (default: {DEFAULT_CHANNEL})')
    parser.add_argument('--csv-dir', default=DEFAULT_CSV_DIR,
                        help='Directory containing time_domain_python_artifact_ch*.csv files')
    parser.add_argument('--file', default=None,
                        help='Direct path to CSV file (overrides --ch / --csv-dir)')
    parser.add_argument('--bw', type=float, default=SIGNAL_BW_HZ,
                        help=f'±Hz around tone counted as signal (default: {SIGNAL_BW_HZ})')
    parser.add_argument('--band', type=float, nargs=2, default=list(DEFAULT_BAND),
                        metavar=('FLO', 'FHI'),
                        help=f'In-band frequency range for SNR (default: {DEFAULT_BAND[0]} {DEFAULT_BAND[1]} Hz)')
    parser.add_argument('--good-only', action='store_true',
                        help='Only use samples with quality code != 0 (valid packets)')
    args = parser.parse_args()

    csv_path = args.file or f'{args.csv_dir}/time_domain_python_artifact_ch{args.ch}.csv'

    print(f'Loading: {csv_path}')
    df = load_csv(csv_path)

    if args.good_only:
        before = len(df)
        df = df[df['quality_code'] != 0]
        print(f'Quality filter: {before} → {len(df)} samples retained')

    amplitude = df['amplitude'].to_numpy(dtype=np.float64)

    # Estimate sample rate from time column (more robust than assuming 200 Hz)
    dt = np.diff(df['time_s'].to_numpy())
    fs = 1.0 / np.median(dt)
    print(f'Estimated sample rate: {fs:.2f} Hz  ({len(amplitude)} samples)')

    result = calc_snr(amplitude, fs, args.freq, bw_hz=args.bw, band=tuple(args.band))

    flo, fhi = result['band_hz']
    rms_noise = np.sqrt(result['noise_power'])
    rms_signal = np.sqrt(result['signal_power'])
    peak_amplitude = rms_signal * np.sqrt(2)

    print()
    print(f'  Analysis band:       {flo:.1f} – {fhi:.1f} Hz')
    print(f'  Tone frequency:      {result["tone_freq_hz"]:.3f} Hz')
    print(f'  Peak FFT bin:        {result["peak_freq_hz"]:.3f} Hz')
    print(f'  FFT resolution:      {result["fft_freq_resolution_hz"]:.4f} Hz/bin')
    print(f'  Signal bandwidth:    ±{result["signal_bw_hz"]:.1f} Hz')
    print(f'  Signal power:        {result["signal_power"]:.6e}')
    print(f'  Noise power:         {result["noise_power"]:.6e}')
    print(f'  RMS signal:          {rms_signal:.6e}')
    print(f'  RMS noise:           {rms_noise:.6e}')
    print(f'  Sinusoid amplitude:  {peak_amplitude:.6e}  (= RMS × √2)')
    print(f'  SNR:                 {result["snr_db"]:.2f} dB')


if __name__ == '__main__':
    main()
