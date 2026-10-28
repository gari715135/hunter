# ./src/signal_analysis/signal_transformations.py
import numpy as np


class SignalTransformations:
    """
    A class to handle FFT and thresholding logic.
    """

    @staticmethod
    def apply_window(data: np.ndarray, window_type: str = "hamming") -> np.ndarray:
        n = len(data)
        if isinstance(window_type, str):
            if window_type.lower() == "hamming":
                window = np.hamming(n)
            elif window_type.lower() == "hanning":
                window = np.hanning(n)
            elif window_type.lower() == "blackman":
                window = np.blackman(n)
            else:
                window = np.ones(n)
        else:
            window = np.ones(n)
        return data * window

    @staticmethod
    def compute_fft(
        data: np.ndarray,
        time_arr: np.ndarray,
        window_type: str = "hamming",
        freq_units=1,
    ):
        """
        Compute the Fast Fourier Transform of the dataset.
        """
        # --- Frequency Domain Subplot ---
        n = len(data)

        t_diffs = np.diff(time_arr) / np.timedelta64(1, "s")
        if np.shape(np.unique(t_diffs))[0] != 1:
            raise ValueError(f"Time differences not uniform, check query results: {t_diffs}")

        bin_size = t_diffs[0]

        freq_hz = np.fft.rfftfreq(n, d=bin_size)
        freq_resample = freq_units * freq_hz

        windowed_data = SignalTransformations.apply_window(data, window_type)

        fft_data = np.fft.rfft(windowed_data)
        magnitude = np.abs(fft_data)

        return magnitude, freq_resample

    @staticmethod
    def compute_fft_threshold(
        fft_mag: np.ndarray,
        offset_pct: int = 0.1,
        threshold_factor: float = 4.0,
    ) -> float:
        """
        Compute a threshold based on (mean + threshold_factor*std) of the high-frequency tail,
        ignoring the first 'offset' points in the FFT.
        """
        if len(fft_mag) == 0:
            return 0.0

        # Ignore the low-frequency portion using a percentage
        offset = int(np.floor(offset_pct * len(fft_mag)))
        tail = fft_mag[offset:] if offset < len(fft_mag) else fft_mag

        mag_tail = np.abs(tail)

        mean_val = np.mean(mag_tail)
        std_val = np.std(mag_tail)

        threshold = mean_val + threshold_factor * std_val

        return float(threshold)

    @staticmethod
    def is_suspicious_signal(
        fft_mag: np.ndarray,
        threshold: float,
        offset_pct: int = 0.1,
    ) -> bool:
        """
        Determine if the data's FFT surpasses the given threshold in the tail region.
        """
        if len(fft_mag) == 0:
            return False

        # Ignore the low-frequency portion
        offset = int(np.floor(offset_pct * len(fft_mag)))
        tail = fft_mag[offset:] if offset < len(fft_mag) else fft_mag

        return np.any(np.abs(tail) > threshold)
