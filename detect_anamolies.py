#!/usr/bin/env python3
"""
detect_anomalies.py

Loads daily token usage data, detects anomalies using the Anomaly class, 
and saves + visualizes results.
"""

import pandas as pd
import matplotlib.pyplot as plt
from flow_graph.anomaly import Anomaly  # assumes your class is saved in anomaly.py


def detect_anomalies(input_csv: str, output_csv: str, method: str = "median_spike", threshold: float = 3.0):
    """
    Detects anomalies in token usage data and saves the output with anomaly flags.

    Args:
        input_csv: Path to the input CSV with columns ['date', 'day_name', 'token_usage'].
        output_csv: Path to save the output CSV with anomalies.
        method: Anomaly detection method ('z_score', 'rolling_z', 'iqr', 'median_spike').
        threshold: Threshold for anomaly detection.
    """
    print(f"📂 Loading data from {input_csv}...")
    df = pd.read_csv(input_csv)

    # Ensure proper date parsing
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.sort_values("date").reset_index(drop=True)

    print(f"✅ Loaded {len(df)} records from {df['date'].min().date()} to {df['date'].max().date()}")

    # Run anomaly detection
    print(f"🚀 Running anomaly detection using '{method}' (threshold={threshold})...")
    a = Anomaly(df)
    out = a.run(field="token_usage", method=method, threshold=threshold, params={"window": 7})

    # Save output
    # out.to_csv(output_csv, index=False)
    # print(f"💾 Saved results to {output_csv}")

    # Summary
    num_anomalies = out["is_anomaly"].sum()
    print(f"⚠️ Detected {num_anomalies} anomalies out of {len(out)} records.")

    # Optional: Plot results
    # plt.figure(figsize=(14, 6))
    # plt.plot(out["date"], out["token_usage"], label="Token Usage", color="blue", linewidth=1.5)
    # plt.scatter(out.loc[out["is_anomaly"], "date"],
    #             out.loc[out["is_anomaly"], "token_usage"],
    #             color="red", label="Anomalies", s=40, zorder=5)
    # plt.title(f"Anomaly Detection ({method})")
    # plt.xlabel("Date")
    # plt.ylabel("Token Usage")
    # plt.legend()
    # plt.grid(True, alpha=0.3)
    # plt.tight_layout()
    # plt.show()

    # --- Plot results ---
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(out["date"], out["token_usage"], label="Token Usage", color="blue", linewidth=1.5)

    anomalies = out.loc[out["is_anomaly"] == True]
    if not anomalies.empty:
        ax.scatter(anomalies["date"], anomalies["token_usage"],
                   color="red", label="Anomalies", s=50, zorder=5)
    else:
        print("No anomalies detected — nothing to highlight on plot.")

    ax.set_title(f"Anomaly Detection ({method})")
    ax.set_xlabel("Date")
    ax.set_ylabel("Token Usage")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    # Save before showing — ensures it's not cleared
    plot_path = output_csv.replace(".csv", "_anomalies.png")
    plt.savefig(plot_path, dpi=150)
    print(f"📊 Anomaly plot saved to {plot_path}")

    return out


if __name__ == "__main__":
    input_csv = "token_usage_logs.csv"      # path to your data
    output_csv = "token.csv"  # output with flags

    # You can experiment with method and threshold:
    detect_anomalies(input_csv, output_csv, method="median_spike", threshold=4.0)
