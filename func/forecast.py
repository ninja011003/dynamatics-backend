import pandas as pd
import numpy as np
from typing import Optional, List


class Forecast:
    """
    Simple forecasting node compatible with the project's node pattern.

    Usage:
        f = Forecast(df)
        preds = f.run(target="value", ts_col="ds", method="moving_average", horizon=5, window=3)

    Methods implemented:
      - naive: last-value carry-forward
      - mean: mean of training values
      - moving_average: rolling mean
      - exp_smoothing: simple exponential smoothing
      - linear_trend: linear regression on time index (uses np.polyfit)
    """

    def __init__(self, input: pd.DataFrame):
        if not isinstance(input, pd.DataFrame):
            raise ValueError("Forecast requires a pandas DataFrame as input")
        self.input = input.copy()
        self.output = None

    def _validate(self, ts_col: str, target: str):
        if ts_col not in self.input.columns:
            raise ValueError(f"Time column '{ts_col}' not found in DataFrame")
        if target not in self.input.columns:
            raise ValueError(f"Target column '{target}' not found in DataFrame")

    def run(
        self,
        target: str,
        ts_col: str,
        method: str = "naive",
        horizon: int = 1,
        freq: Optional[str] = None,
        window: int = 3,
        alpha: float = 0.2,
    ) -> pd.DataFrame:
        """
        Generate forecasts for the given horizon.

        Args:
            target: name of the target column to forecast.
            ts_col: name of datetime/time index column.
            method: forecasting method. One of: 'naive', 'mean', 'moving_average', 'exp_smoothing', 'linear_trend'.
            horizon: how many steps to forecast ahead (int).
            freq: optional pandas frequency string to build future timestamps. If None and ts_col is datetime, infer freq.
            window: window size for moving average.
            alpha: smoothing factor for exponential smoothing (0-1).

        Returns:
            DataFrame with forecasted timestamps and predicted values in column 'forecast'.
        """
        self._validate(ts_col, target)

        df = self.input[[ts_col, target]].dropna().copy()

        # ensure ts_col is datetime if possible
        if not np.issubdtype(df[ts_col].dtype, np.datetime64):
            try:
                df[ts_col] = pd.to_datetime(df[ts_col])
            except Exception:
                # leave as-is; linear_trend will use integer index
                pass

        df = df.sort_values(ts_col).reset_index(drop=True)

        # build future index
        last_ts = df[ts_col].iloc[-1]
        future_idx = None
        if pd.api.types.is_datetime64_any_dtype(df[ts_col].dtype):
            if freq is None:
                inferred = pd.infer_freq(df[ts_col])
                freq = inferred if inferred is not None else 'D'
            future_idx = pd.date_range(start=last_ts, periods=horizon + 1, freq=freq)[1:]
        else:
            # use integer steps
            future_idx = [df.index[-1] + i for i in range(1, horizon + 1)]

        y = df[target].values

        if method == 'naive':
            preds = [y[-1]] * horizon

        elif method == 'mean':
            preds = [float(np.mean(y))] * horizon

        elif method == 'moving_average':
            if window < 1:
                raise ValueError("window must be >= 1")
            if len(y) < 1:
                preds = [np.nan] * horizon
            else:
                last_vals = y[max(0, len(y) - window):]
                mean_val = float(np.mean(last_vals))
                preds = [mean_val] * horizon

        elif method == 'exp_smoothing':
            # simple exponential smoothing
            if not (0 < alpha <= 1):
                raise ValueError("alpha must be in (0,1]")
            s = y[0] if len(y) > 0 else 0.0
            for val in y:
                s = alpha * val + (1 - alpha) * s
            preds = [s] * horizon

        elif method == 'linear_trend':
            # fit line y = a*x + b on integer index
            if len(y) == 0:
                preds = [np.nan] * horizon
            else:
                x = np.arange(len(y)).astype(float)
                # use polyfit degree 1
                a, b = np.polyfit(x, y, 1)
                future_x = np.arange(len(y), len(y) + horizon)
                preds = list((a * future_x + b).astype(float))

        else:
            raise ValueError(f"Unsupported method: {method}")

        out_df = pd.DataFrame({ts_col: future_idx, 'forecast': preds})
        self.output = out_df
        return out_df


if __name__ == '__main__':
    # quick demo
    dates = pd.date_range('2020-01-01', periods=10, freq='D')
    df = pd.DataFrame({'ds': dates, 'value': np.arange(10) + np.random.randn(10) * 0.5})
    f = Forecast(df)
    print('\nOriginal:\n', df.tail())
    print('\nNaive forecast:\n', f.run(target='value', ts_col='ds', method='naive', horizon=5))
    print('\nMoving average forecast:\n', f.run(target='value', ts_col='ds', method='moving_average', horizon=5, window=3))
