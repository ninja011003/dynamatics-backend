#!/usr/bin/env python3
"""
Forecast token usage for the next N days (default 30).
- Reads NDJSON logs (one JSON object per line).
- Aggregates tokens_used by day.
- Uses the provided Forecast class to predict next N days.
- Ensures forecast rows have real calendar dates (starting from today or last log date).
- Saves a plot with historic + forecast.
"""

import json
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from flow_graph.forecast import Forecast


def load_logs(json_path: str) -> pd.DataFrame:
    """
    Reads NDJSON logs and extracts timestamp + token usage info.
    Returns DataFrame with columns ['timestamp', 'tokens_used'] where timestamp is datetime.
    """
    records = []
    with open(json_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                log = json.loads(line)
            except Exception:
                continue

            # look for token usages in common keys
            tokens = (
                log.get("brand_kit.content_generations.tokens_used")
                or log.get("brand_kit.voice_profiles.tokens_used")
                or log.get("tokens_used")
            )
            # choose a timestamp key (prioritize per-entry timestamp if present)
            ts = (
                log.get("brand_kit.content_generations.timestamp")
                or log.get("brand_kit.voice_profiles.timestamp")
                or log.get("timestamp")
            )

            if ts is None or tokens is None:
                continue

            try:
                ts_parsed = pd.to_datetime(ts, errors="coerce")
                if pd.isna(ts_parsed):
                    continue
            except Exception:
                continue

            try:
                tokens_val = float(tokens)
            except Exception:
                # skip non-numeric token values
                continue

            records.append({"timestamp": ts_parsed, "tokens_used": tokens_val})

    if not records:
        raise ValueError("No valid token usage records found in the log file.")

    df = pd.DataFrame(records)
    # normalize to date resolution (group by day). If you want to keep higher-res, remove .dt.floor('D')
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False)
    # aggregate by date (day). If your logs are hourly/minute and you want that resolution, change this.
    df = df.assign(timestamp=df["timestamp"].dt.floor("D"))
    df = df.groupby("timestamp", as_index=False)["tokens_used"].sum().sort_values("timestamp").reset_index(drop=True)
    return df


def forecast_tokens(df: pd.DataFrame, horizon: int = 30) -> pd.DataFrame:
    """
    Runs Forecast on df to predict next `horizon` days.
    Ensures forecast rows contain real calendar dates starting from today or last_date (whichever is later).
    Returns the combined DataFrame with columns: ['timestamp', 'forecast', 'source'].
    """
    print("Running forecast...")

    # --- FIX: unify timezone to UTC before doing anything ---
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # the Forecast class expects input_df to have the timestamp column named exactly "timestamp"
    f = Forecast(df.copy())

    combined = f.run(
        target="tokens_used",
        ts_col="timestamp",
        method="holt_winters",
        horizon=horizon,
        window=7,
        seasonal_periods=30,
        combine=False
    )

    result_df = pd.DataFrame(combined).copy()

    if "timestamp" not in result_df.columns:
        raise ValueError("Forecast output missing timestamp column")

    # --- FIX: convert result timestamps to UTC too ---
    result_df["timestamp"] = pd.to_datetime(result_df["timestamp"], utc=True, errors="coerce")

    last_log_date = df["timestamp"].max()
    today = pd.Timestamp.now(tz="UTC").floor("D")  # --- FIX: ensure UTC consistency ---
    start_date = max(last_log_date, today)

    if "source" in result_df.columns:
        mask_forecast = result_df["source"] == "forecast"
    else:
        mask_forecast = pd.Series([False] * len(result_df))
        if len(result_df) >= horizon:
            mask_forecast.iloc[-horizon:] = True

    num_forecast_rows = int(mask_forecast.sum())
    if num_forecast_rows > 0:
        # --- FIX: generate future dates in UTC timezone ---
        future_dates = pd.date_range(start=start_date, periods=num_forecast_rows + 1, freq="D", tz="UTC")[1:]
        result_df.loc[mask_forecast, "timestamp"] = future_dates.values

    # --- FIX: ensure timestamps stay UTC ---
    result_df["timestamp"] = pd.to_datetime(result_df["timestamp"], utc=True, errors="coerce")

    result_df = result_df.sort_values("timestamp").reset_index(drop=True)

    print(f"Historic rows: {(result_df['source'] == 'history').sum() if 'source' in result_df.columns else 'N/A'}")
    print(f"Forecast rows: {num_forecast_rows}")
    print("Date range after fix:", result_df["timestamp"].min(), "->", result_df["timestamp"].max())

    return result_df

def plot_forecast(result_df: pd.DataFrame, save_path: str = "forecast_plot.png"):
    """
    Plot historic (source=='history') and forecast (source=='forecast') token usage.
    If 'source' column missing, plot everything as a single series.
    """
    plt.figure(figsize=(12, 6))

    if "source" in result_df.columns:
        hist = result_df[result_df["source"] == "history"]
        fut = result_df[result_df["source"] == "forecast"]
        # historic values are stored in 'forecast' column (Forecast.combine sets history target renamed to 'forecast')
        plt.plot(hist["timestamp"], hist["forecast"], label="Historic", marker="o")
        plt.plot(fut["timestamp"], fut["forecast"], label="Forecast", marker="x", linestyle="--")
    else:
        # fallback: if Forecast didn't provide 'source', just plot whatever columns we can
        if "tokens_used" in result_df.columns:
            plt.plot(result_df["timestamp"], result_df["tokens_used"], label="Tokens", marker="o")
        elif "forecast" in result_df.columns:
            plt.plot(result_df["timestamp"], result_df["forecast"], label="Forecast", marker="o")
        else:
            raise ValueError("No recognized column to plot (need 'forecast' or 'tokens_used')")

    plt.title("Token Usage Forecast (Next {} Days)".format(
        (result_df[result_df["source"] == "forecast"].shape[0] if "source" in result_df.columns else ""))
    )
    plt.xlabel("Date")
    plt.ylabel("Tokens Used")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(save_path)
    plt.show()
    print(f"Forecast plot saved to {save_path}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Forecast token usage from NDJSON logs.")
    parser.add_argument("--input", type=str, required=True, help="Path to NDJSON log file")
    parser.add_argument("--horizon", type=int, default=30, help="Days to forecast")
    parser.add_argument("--output", type=str, default="forecast_plot.png", help="Path to save the plot")
    args = parser.parse_args()

    print(f"Loading logs from {args.input}...")
    df = load_logs(args.input)
    print(f"Loaded {len(df)} daily token usage records (from {df['timestamp'].min()} to {df['timestamp'].max()}).")

    result_df = forecast_tokens(df, horizon=args.horizon)

    print("Plotting results...")
    plot_forecast(result_df, save_path=args.output)


if __name__ == "__main__":
    main()
