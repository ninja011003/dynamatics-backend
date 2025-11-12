#!/usr/bin/env python3
"""
Script to auto-generate time series data for testing forecast functionality
"""
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_timeseries_data(
    start_date='2024-01-01',
    periods=90,
    freq='D',
    base_value=1000,
    trend_slope=2.0,
    seasonal_amplitude=100,
    seasonal_period=7,
    noise_level=20,
    output_file='timeseries.ndjson'
):
    """
    Generate synthetic time series data with trend, seasonality, and noise.
    
    Parameters:
    -----------
    start_date : str
        Starting date in 'YYYY-MM-DD' format
    periods : int
        Number of time periods to generate
    freq : str
        Frequency ('D' for daily, 'W' for weekly, 'M' for monthly)
    base_value : float
        Base value for the time series
    trend_slope : float
        Linear trend component (value increase per period)
    seasonal_amplitude : float
        Amplitude of seasonal oscillations
    seasonal_period : int
        Period of seasonality (e.g., 7 for weekly, 30 for monthly)
    noise_level : float
        Standard deviation of random noise
    output_file : str
        Output file name
    """
    
    # Generate date range
    dates = pd.date_range(start=start_date, periods=periods, freq=freq)
    
    # Generate time series components
    t = np.arange(periods)
    
    # Trend component (linear increase)
    trend = trend_slope * t
    
    # Seasonal component (sinusoidal pattern)
    seasonal = seasonal_amplitude * np.sin(2 * np.pi * t / seasonal_period)
    
    # Random noise
    noise = np.random.randn(periods) * noise_level
    
    # Combine all components
    cost_values = base_value + trend + seasonal + noise
    
    # Generate correlated revenue (cost * 2 + some variation)
    revenue_values = cost_values * 2.0 + np.random.randn(periods) * (noise_level * 0.5)
    
    # Create DataFrame
    df = pd.DataFrame({
        'date': dates.strftime('%Y-%m-%d'),
        'cost': cost_values.round(2),
        'revenue': revenue_values.round(2),
        'product': 'Product A'
    })
    
    # Save to NDJSON format
    with open(output_file, 'w') as f:
        for _, row in df.iterrows():
            json.dump(row.to_dict(), f)
            f.write('\n')
    
    print(f"✓ Generated {periods} time series records")
    print(f"✓ Saved to: {output_file}")
    print(f"\nData characteristics:")
    print(f"  - Date range: {df['date'].iloc[0]} to {df['date'].iloc[-1]}")
    print(f"  - Cost range: ${df['cost'].min():.2f} - ${df['cost'].max():.2f}")
    print(f"  - Revenue range: ${df['revenue'].min():.2f} - ${df['revenue'].max():.2f}")
    print(f"  - Trend: {trend_slope:.2f} per period")
    print(f"  - Seasonality: {seasonal_period} periods")
    
    return df


def generate_multiple_products(
    start_date='2024-01-01',
    periods=90,
    products=['Product A', 'Product B', 'Product C'],
    output_file='timeseries_multi.ndjson'
):
    """
    Generate time series data for multiple products.
    """
    all_data = []
    
    for i, product in enumerate(products):
        # Vary parameters slightly for each product
        base_value = 1000 + (i * 200)
        trend_slope = 1.5 + (i * 0.5)
        seasonal_amplitude = 80 + (i * 20)
        
        dates = pd.date_range(start=start_date, periods=periods, freq='D')
        t = np.arange(periods)
        
        trend = trend_slope * t
        seasonal = seasonal_amplitude * np.sin(2 * np.pi * t / 7)
        noise = np.random.randn(periods) * 15
        
        cost_values = base_value + trend + seasonal + noise
        revenue_values = cost_values * (2.0 + i * 0.1) + np.random.randn(periods) * 10
        
        for j in range(periods):
            all_data.append({
                'date': dates[j].strftime('%Y-%m-%d'),
                'cost': round(cost_values[j], 2),
                'revenue': round(revenue_values[j], 2),
                'product': product
            })
    
    # Save to NDJSON format
    with open(output_file, 'w') as f:
        for record in all_data:
            json.dump(record, f)
            f.write('\n')
    
    print(f"✓ Generated {len(all_data)} records for {len(products)} products")
    print(f"✓ Saved to: {output_file}")
    
    return pd.DataFrame(all_data)


if __name__ == '__main__':
    print("=" * 70)
    print("TIME SERIES DATA GENERATOR")
    print("=" * 70)
    
    print("\n1. Generating single product time series (60 days)...")
    df1 = generate_timeseries_data(
        start_date='2024-01-01',
        periods=60,
        freq='D',
        base_value=1200,
        trend_slope=3.0,
        seasonal_amplitude=150,
        seasonal_period=7,
        noise_level=25,
        output_file='timeseries.ndjson'
    )
    
    print("\n2. Generating multi-product time series (90 days)...")
    df2 = generate_multiple_products(
        start_date='2024-01-01',
        periods=90,
        products=['Product A', 'Product B', 'Product C'],
        output_file='timeseries_multi.ndjson'
    )
    
    print("\n3. Generating long-term time series (365 days)...")
    df3 = generate_timeseries_data(
        start_date='2023-01-01',
        periods=365,
        freq='D',
        base_value=1500,
        trend_slope=1.5,
        seasonal_amplitude=200,
        seasonal_period=30,  # Monthly seasonality
        noise_level=30,
        output_file='timeseries_long.ndjson'
    )
    
    print("\n" + "=" * 70)
    print("Time series data generation completed!")
    print("=" * 70)
    print("\nGenerated files:")
    print("  • timeseries.ndjson - Single product, 60 days, weekly seasonality")
    print("  • timeseries_multi.ndjson - 3 products, 90 days each")
    print("  • timeseries_long.ndjson - Single product, 365 days, monthly seasonality")
    print("\nYou can now use these files with the forecast node!")
    print("=" * 70)

