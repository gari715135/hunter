# ./src/anomaly_bands/band_transformations.py
import numpy as np
import pandas as pd


class BandTransformations:

    @staticmethod
    def apply_window(data: np.ndarray, window_type: str = "hamming") -> np.ndarray:
        n = len(data)
        if window_type.lower() == "hamming":
            window = np.hamming(n)
        elif window_type.lower() == "hanning":
            window = np.hanning(n)
        elif window_type.lower() == "blackman":
            window = np.blackman(n)
        else:
            window = np.ones(n)
        return data * window

    @staticmethod
    def compute_rolling(
        df: pd.DataFrame, n_stds: int, rolling_window="120min", window_type: str = None
    ):
        """
        Compute the Fast Fourier Transform of the dataset.
        """
        if window_type:
            df["value"] = BandTransformations.apply_window(
                df["value"].to_numpy(), window_type=window_type
            )

        df_reindex = df.set_index("time").sort_index()

        rolling_obj = df_reindex.rolling(
            window=rolling_window,
        )

        rolling_mean = rolling_obj.mean().rename(columns={"value": "moving"})
        rolling_std = rolling_obj.std().rename(columns={"value": "std"})

        rolling_df = rolling_mean.join(rolling_std)

        rolling_df["b+"] = rolling_df["moving"] + rolling_df["std"] * n_stds
        rolling_df["b-"] = rolling_df["moving"] - rolling_df["std"] * n_stds

        b_minus_positive = rolling_df["b-"] > 0
        b_minus_na = np.isnan(rolling_df["b-"])

        rolling_df["b-"] = np.where((b_minus_positive | b_minus_na), rolling_df["b-"], 0)

        return rolling_df

    @staticmethod
    def is_suspicious_activity(
        df_raw: pd.DataFrame,
        df_rolling: pd.DataFrame,
    ) -> bool:
        """
        Determine if the containers network activity surpasses the given threshold.
        """
        if len(df_raw) == 0 or len(df_rolling) == 0:
            return False

        return np.any(np.abs(df_raw["value"]) > df_rolling["b+"])