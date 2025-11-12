import pandas as pd
import numpy as np
from typing import Optional, Dict, Any


class Anomaly:
    """A lightweight anomaly detection node.

    Usage pattern matches existing nodes in `func/`: instantiate with a
    pandas.DataFrame and call `run(...)` to detect anomalies. The output is
    the input DataFrame with two new columns added:
      - `anomaly_score`: numeric score (higher == more anomalous)
      - `is_anomaly`: boolean flag indicating whether the row is an anomaly

    Supported methods: 'z_score', 'rolling_z', 'iqr', 'median_spike'.
    """

    def __init__(self, input: pd.DataFrame):
        self.input = input.copy().reset_index(drop=True)
        self.output: Optional[pd.DataFrame] = None

    def _z_score(self, field: str) -> pd.Series:
        x = self.input[field].astype(float)
        mu = x.mean()
        sigma = x.std(ddof=0)
        if sigma == 0:
            return pd.Series(np.zeros(len(x)), index=self.input.index)
        return (x - mu).abs() / sigma

    def _rolling_z(self, field: str, window: int = 7, min_periods: int = 3) -> pd.Series:
        x = self.input[field].astype(float)
        roll_mean = x.rolling(window=window, min_periods=min_periods, center=True).mean()
        roll_std = x.rolling(window=window, min_periods=min_periods, center=True).std(ddof=0)
        score = (x - roll_mean).abs() / roll_std
        # replace NaN (edges) with global z-score
        score = score.fillna(self._z_score(field))
        return score

    def _iqr_score(self, field: str, window: Optional[int] = None) -> pd.Series:
        x = self.input[field].astype(float)
        if window and window > 1:
            q1 = x.rolling(window=window, min_periods=1, center=True).quantile(0.25)
            q3 = x.rolling(window=window, min_periods=1, center=True).quantile(0.75)
        else:
            q1 = x.quantile(0.25)
            q3 = x.quantile(0.75)
        iqr = (q3 - q1).replace(0, np.nan)
        # distance from nearest quartile normalized by IQR
        lower_dist = (q1 - x).clip(lower=0)
        upper_dist = (x - q3).clip(lower=0)
        dist = pd.concat([lower_dist, upper_dist], axis=1).max(axis=1)
        score = dist / iqr
        score = score.replace([np.inf, -np.inf], np.nan).fillna(0)
        return score

    def _median_spike(self, field: str, window: int = 7) -> pd.Series:
        """Detect spikes by comparing to local median and MAD (robust)."""
        x = self.input[field].astype(float)
        med = x.rolling(window=window, min_periods=1, center=True).median()
        mad = (x - med).abs().rolling(window=window, min_periods=1, center=True).median()
        mad = mad.replace(0, np.nan)
        score = (x - med).abs() / mad
        score = score.fillna(0)
        return score

    def run(self, field: str, method: str = "z_score", threshold: float = 3.0, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Run anomaly detection.

        Args:
            field: column name in `input` to analyze
            method: one of ['z_score','rolling_z','iqr','median_spike']
            threshold: numeric threshold above which a point is considered an anomaly
            params: extra method-specific parameters (e.g., window size)

        Returns:
            DataFrame copy with `anomaly_score` and `is_anomaly` columns.
        """
        if params is None:
            params = {}

        if field not in self.input.columns:
            raise ValueError(f"Field '{field}' not found in input DataFrame")

        if method == "z_score":
            score = self._z_score(field)
        elif method == "rolling_z":
            window = int(params.get("window", 7))
            min_periods = int(params.get("min_periods", max(3, window // 3)))
            score = self._rolling_z(field, window=window, min_periods=min_periods)
        elif method == "iqr":
            window = params.get("window", None)
            score = self._iqr_score(field, window=window)
        elif method == "median_spike":
            window = int(params.get("window", 7))
            score = self._median_spike(field, window=window)
        else:
            raise ValueError(f"Unknown method: {method}")

        # Fill any NaNs with 0
        score = score.fillna(0).astype(float)

        out = self.input.copy()
        out["anomaly_score"] = score
        out["is_anomaly"] = out["anomaly_score"] > float(threshold)

        self.output = out
        return self.output


if __name__ == "__main__":
    # simple demo
    import math

    rng = np.random.default_rng(42)
    n = 200
    ts = np.sin(np.linspace(0, 6 * math.pi, n)) + rng.normal(0, 0.2, size=n)
    # inject spikes
    ts[20] += 5
    ts[80] -= 6
    df = pd.DataFrame({"ts": ts, "t": pd.date_range("2020-01-01", periods=n)})
    print("Input data head:\n", df)

    a = Anomaly(df)
    out = a.run(field="ts", method="rolling_z", threshold=3.0, params={"window": 11})
    print(out[out["is_anomaly"]].head(20))
