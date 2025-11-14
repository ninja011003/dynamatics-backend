#############################################################################
#This file was written by Vidya-charan-cs
#############################################################################


import pandas as pd
import numpy as np
from typing import Optional, Tuple, Dict, List

try:
    from statsmodels.tsa.holtwinters import SimpleExpSmoothing, Holt, ExponentialSmoothing
    STATSMODELS_HW_AVAILABLE = True
except Exception:
    STATSMODELS_HW_AVAILABLE = False

try:
    from statsmodels.tsa.arima.model import ARIMA
    ARIMA_AVAILABLE = True
except Exception:
    ARIMA_AVAILABLE = False


class Forecast:
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

    @staticmethod
    def _infer_freq(series: pd.Series) -> Optional[str]:
        freq = pd.infer_freq(series)
        if freq is not None:
            return freq
        diffs = series.diff().dropna()
        if len(diffs) == 0:
            return None
        if isinstance(diffs.iloc[0], pd.Timedelta):
            median = diffs.median()
            if median % pd.Timedelta(days=1) == pd.Timedelta(0):
                days = median // pd.Timedelta(days=1)
                return f'{int(days)}D'
            if median % pd.Timedelta(hours=1) == pd.Timedelta(0):
                hours = median // pd.Timedelta(hours=1)
                return f'{int(hours)}H'
            if median % pd.Timedelta(minutes=1) == pd.Timedelta(0):
                mins = median // pd.Timedelta(minutes=1)
                return f'{int(mins)}T'
            return None
        return None

    @staticmethod
    def _detect_seasonal_periods(data: np.ndarray, max_period: int = 50) -> int:
        n = len(data)
        if n < 4:
            return 7
        max_period = min(max_period, n // 2)
        if max_period < 2:
            return 7
        
        autocorr = []
        mean_val = np.mean(data)
        var_val = np.var(data)
        
        if var_val == 0:
            return 7
        
        for lag in range(1, max_period + 1):
            c = np.mean((data[:-lag] - mean_val) * (data[lag:] - mean_val))
            autocorr.append(c / var_val)
        
        autocorr = np.array(autocorr)
        peaks = []
        for i in range(1, len(autocorr) - 1):
            if autocorr[i] > autocorr[i-1] and autocorr[i] > autocorr[i+1] and autocorr[i] > 0.1:
                peaks.append((i + 1, autocorr[i]))
        
        if peaks:
            peaks.sort(key=lambda x: x[1], reverse=True)
            detected_period = peaks[0][0]
            if 2 <= detected_period <= max_period:
                return detected_period
        
        if n >= 365:
            return 365
        elif n >= 52:
            return 52
        elif n >= 30:
            return 30
        elif n >= 14:
            return 14
        else:
            return 7

    def predict(
        self,
        target: str,
        ts_col: str,
        horizon: int = 30,
        seasonal_periods: Optional[int] = None,
    ) -> List[Dict]:
        if not STATSMODELS_HW_AVAILABLE:
            raise ImportError("statsmodels is required for forecasting. Install: pip install statsmodels")
        
        self._validate(ts_col, target)
        df = self.input[[ts_col, target]].dropna().copy()
        
        try:
            if not np.issubdtype(df[ts_col].dtype, np.datetime64):
                df[ts_col] = pd.to_datetime(df[ts_col])
        except Exception:
            pass
        
        df = df.sort_values(ts_col).reset_index(drop=True)
        y = df[target].values.astype(float)
        
        if seasonal_periods is None:
            seasonal_periods = self._detect_seasonal_periods(y)
        
        min_data_needed = seasonal_periods * 2
        if len(y) < min_data_needed:
            seasonal_periods = max(2, len(y) // 3)
        
        try:
            result = self.run(
                target=target,
                ts_col=ts_col,
                method='hw',
                horizon=horizon,
                seasonal_periods=seasonal_periods,
                combine=True
            )
        except (ValueError, Exception) as e:
            result = self.run(
                target=target,
                ts_col=ts_col,
                method='holt',
                horizon=horizon,
                combine=True
            )
        
        output = []
        for _, row in result.iterrows():
            output.append({
                'date': row[ts_col],
                'value': float(row['forecast']),
                'is_historic': row['source'] == 'history'
            })
        
        return output

    def run(
        self,
        target: str,
        ts_col: str,
        method: str = "holt_winters",
        horizon: int = 1,
        freq: Optional[str] = None,
        window: int = 3,
        alpha: float = 0.2,
        order: Tuple[int, int, int] = (1, 1, 1),
        seasonal_periods: Optional[int] = None,
        combine: bool = False,
    ) -> pd.DataFrame:
        """
        Runs forecast and returns DataFrame with columns [ts_col, 'forecast'].
        If combine=True returns concatenated historical rows then forecast rows (useful for plotting).
        """
        self._validate(ts_col, target)
        df = self.input[[ts_col, target]].dropna().copy()

        is_datetime = False
        try:
            if not np.issubdtype(df[ts_col].dtype, np.datetime64):
                df[ts_col] = pd.to_datetime(df[ts_col])
            is_datetime = pd.api.types.is_datetime64_any_dtype(df[ts_col].dtype)
        except Exception:
            is_datetime = False

        df = df.sort_values(ts_col).reset_index(drop=True)
        y = df[target].values.astype(float)

        last_ts = df[ts_col].iloc[-1]
        if is_datetime:
            if freq is None:
                freq = self._infer_freq(df[ts_col])
                if freq is None:
                    freq = 'D'
            future_idx = pd.date_range(start=last_ts, periods=horizon + 1, freq=freq)[1:]
        else:
            future_idx = [df.index[-1] + i for i in range(1, horizon + 1)]

        preds = None

        if method == 'naive':
            preds = [float(y[-1])] * horizon

        elif method == 'mean':
            preds = [float(np.mean(y))] * horizon

        elif method == 'moving_average':
            if window < 1:
                raise ValueError("window must be >= 1")
            buffer = list(y[max(0, len(y) - window):].astype(float))
            preds = []
            for _ in range(horizon):
                if len(buffer) == 0:
                    val = np.nan
                else:
                    val = float(np.mean(buffer[-window:]))
                preds.append(val)
                buffer.append(val)

        elif method == 'exp_smoothing':
            if not (0 < alpha <= 1):
                raise ValueError("alpha must be in (0,1]")
            if len(y) == 0:
                preds = [np.nan] * horizon
            else:
                s = y[0]
                for val in y:
                    s = alpha * val + (1 - alpha) * s
                preds = [float(s)] * horizon

        elif method == 'holt':
            if not STATSMODELS_HW_AVAILABLE:
                raise ImportError("statsmodels is required for Holt method. Install: pip install statsmodels")
            model = Holt(y)
            fitted = model.fit(optimized=True)
            preds = fitted.forecast(horizon).tolist()

        elif method == 'hw' or method == 'holt_winters':
            if not STATSMODELS_HW_AVAILABLE:
                raise ImportError("statsmodels is required for Holt-Winters method. Install: pip install statsmodels")
            if seasonal_periods is None:
                raise ValueError("seasonal_periods must be provided for Holt-Winters")
            model = ExponentialSmoothing(y, seasonal_periods=seasonal_periods, trend='add', seasonal='add', initialization_method="estimated")
            fitted = model.fit(optimized=True)
            preds = fitted.forecast(horizon).tolist()

        elif method == 'linear_trend':
            if len(y) == 0:
                preds = [np.nan] * horizon
            else:
                if is_datetime:
                    x_raw = df[ts_col].map(pd.Timestamp.toordinal).astype(float).values
                    x_mean = x_raw.mean()
                    x_std = x_raw.std() if x_raw.std() != 0 else 1.0
                    x = (x_raw - x_mean) / x_std
                    future_ts_ord = pd.to_datetime(future_idx).map(pd.Timestamp.toordinal).astype(float)
                    future_x = (future_ts_ord - x_mean) / x_std
                else:
                    x = np.arange(len(y)).astype(float)
                    future_x = np.arange(len(y), len(y) + horizon).astype(float)
                a, b = np.polyfit(x, y, 1)
                preds = list((a * future_x + b).astype(float))

        elif method == 'arima':
            if not ARIMA_AVAILABLE:
                raise ImportError("statsmodels is required for ARIMA forecasting. Install it with: pip install statsmodels")
            if len(y) < 3:
                raise ValueError("ARIMA requires at least 3 data points")
            try:
                model = ARIMA(y, order=order)
                fitted_model = model.fit()
                forecast_result = fitted_model.get_forecast(steps=horizon)
                preds = forecast_result.predicted_mean.tolist() if hasattr(forecast_result.predicted_mean, 'tolist') else list(forecast_result.predicted_mean)
            except Exception as e:
                raise ValueError(f"ARIMA fitting failed: {str(e)}. Try different order parameters.")

        else:
            raise ValueError(f"Unsupported method: {method}")

        out_df = pd.DataFrame({ts_col: future_idx, 'forecast': preds})
        
        if combine:
            hist = df.rename(columns={target: 'forecast'})[[ts_col, 'forecast']].copy()
            hist['source'] = 'history'
            out_df2 = out_df.copy()
            out_df2['source'] = 'forecast'
            combined = pd.concat([hist, out_df2], ignore_index=True)
            self.output = combined
            return combined
        
        self.output = out_df
        return out_df


if __name__ == '__main__':
    import json
    
    dates = pd.date_range('2020-01-01', periods=100, freq='D')
    seasonal = 3 * np.sin(2 * np.pi * np.arange(len(dates)) / 14)
    trend = np.linspace(5, 15, len(dates))
    values = trend + seasonal + np.random.randn(len(dates)) * 0.5
    df = pd.DataFrame({'date': dates, 'value': values})

    f = Forecast(df)
    
    print("=" * 70)
    print("HOLT-WINTERS FORECAST (Default Method)")
    print("=" * 70)
    
    result = f.predict(target='value', ts_col='date', horizon=30)
    
    print(f"\nTotal records: {len(result)}")
    print(f"Historic records: {sum(1 for r in result if r['is_historic'])}")
    print(f"Forecast records: {sum(1 for r in result if not r['is_historic'])}")
    
    print("\nLast 5 historic values:")
    historic = [r for r in result if r['is_historic']][-5:]
    for r in historic:
        print(f"  {r['date'].strftime('%Y-%m-%d')}: {r['value']:.2f} (historic)")
    
    print("\nFirst 5 forecast values:")
    forecast = [r for r in result if not r['is_historic']][:5]
    for r in forecast:
        print(f"  {r['date'].strftime('%Y-%m-%d')}: {r['value']:.2f} (forecast)")
    
    print("\nSample JSON output (first forecast value):")
    print(json.dumps(forecast[0], indent=2, default=str))
