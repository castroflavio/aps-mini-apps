#!/usr/bin/env python3
"""
Analyze network counter data from netmonitor.py
"""

import pandas as pd
import numpy as np
import click
from datetime import datetime

def analyze_network_counters(csv_file, expected_gbps=None):
    """Analyze network counter CSV data"""
    
    try:
        # Check if file exists and is readable
        import os
        if not os.path.exists(csv_file):
            print(f"File not found: {csv_file}")
            return
        
        if os.path.getsize(csv_file) == 0:
            print(f"Empty file: {csv_file}")
            return
        
        df = pd.read_csv(csv_file)
        
        if len(df) == 0:
            print(f"No data in file: {csv_file}")
            return
        
        if len(df) < 2:
            print(f"Insufficient data: only {len(df)} samples (need at least 2 for analysis)")
            # Still show what we have
            print(f"Available columns: {list(df.columns)}")
            if len(df) == 1:
                row = df.iloc[0]
                print(f"Single sample: TX={row.get('bytes_sent', 'N/A')} bytes, RX={row.get('bytes_recv', 'N/A')} bytes")
            return
        
        # Convert timestamps
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['elapsed'] = (df['timestamp'] - df['timestamp'].iloc[0]).dt.total_seconds()
        
        # Calculate deltas (differences between consecutive samples)
        df['bytes_sent_delta'] = df['bytes_sent'].diff()
        df['bytes_recv_delta'] = df['bytes_recv'].diff()
        df['packets_sent_delta'] = df['packets_sent'].diff()
        df['packets_recv_delta'] = df['packets_recv'].diff()
        
        # Calculate throughput (bits per second)
        df['tx_bps'] = df['bytes_sent_delta'] * 8
        df['rx_bps'] = df['bytes_recv_delta'] * 8
        df['tx_gbps'] = df['tx_bps'] / 1e9
        df['rx_gbps'] = df['rx_bps'] / 1e9
        
        # Remove first row (no delta available) and any negative deltas (counter resets)
        valid_data = df[(df['bytes_sent_delta'] > 0) | (df['bytes_recv_delta'] > 0)].iloc[1:]
        
        if len(valid_data) == 0:
            print("No valid throughput data found")
            return
        
        # Overall statistics
        total_duration = df['elapsed'].iloc[-1]
        total_sent_gb = (df['bytes_sent'].iloc[-1] - df['bytes_sent'].iloc[0]) / 1e9
        total_recv_gb = (df['bytes_recv'].iloc[-1] - df['bytes_recv'].iloc[0]) / 1e9
        
        print(f"\n=== Network Counter Analysis ===")
        print(f"Duration: {total_duration:.1f}s | Samples: {len(df)}")
        print(f"Total TX: {total_sent_gb:.2f} GB | Total RX: {total_recv_gb:.2f} GB")
        
        # Throughput statistics (only non-zero values)
        tx_nonzero = valid_data[valid_data['tx_gbps'] > 0]['tx_gbps']
        rx_nonzero = valid_data[valid_data['rx_gbps'] > 0]['rx_gbps']
        
        if len(tx_nonzero) > 0:
            tx_mean = tx_nonzero.mean()
            tx_max = tx_nonzero.max()
            tx_std = tx_nonzero.std()
            
            print(f"\nTX Throughput: Avg={tx_mean:.2f} Gbps, Max={tx_max:.2f} Gbps, Std={tx_std:.2f}")
            
            if expected_gbps:
                efficiency = (tx_mean / expected_gbps) * 100
                print(f"TX Efficiency: {efficiency:.1f}% of expected {expected_gbps:.2f} Gbps")
        
        if len(rx_nonzero) > 0:
            rx_mean = rx_nonzero.mean()
            rx_max = rx_nonzero.max()
            rx_std = rx_nonzero.std()
            
            print(f"RX Throughput: Avg={rx_mean:.2f} Gbps, Max={rx_max:.2f} Gbps, Std={rx_std:.2f}")
        
        # Peak activity periods
        peak_tx_idx = valid_data['tx_gbps'].idxmax() if len(tx_nonzero) > 0 else None
        peak_rx_idx = valid_data['rx_gbps'].idxmax() if len(rx_nonzero) > 0 else None
        
        if peak_tx_idx is not None:
            peak_time = valid_data.loc[peak_tx_idx, 'elapsed']
            peak_value = valid_data.loc[peak_tx_idx, 'tx_gbps']
            print(f"Peak TX: {peak_value:.2f} Gbps at {peak_time:.1f}s")
        
        if peak_rx_idx is not None:
            peak_time = valid_data.loc[peak_rx_idx, 'elapsed']
            peak_value = valid_data.loc[peak_rx_idx, 'rx_gbps']
            print(f"Peak RX: {peak_value:.2f} Gbps at {peak_time:.1f}s")
        
        # Error summary
        total_errors = (df['errin'].iloc[-1] - df['errin'].iloc[0] + 
                       df['errout'].iloc[-1] - df['errout'].iloc[0] +
                       df['dropin'].iloc[-1] - df['dropin'].iloc[0] +
                       df['dropout'].iloc[-1] - df['dropout'].iloc[0])
        
        if total_errors > 0:
            print(f"⚠️  Network errors: {total_errors}")
        else:
            print("✅ No network errors detected")
        
    except Exception as e:
        print(f"Analysis error: {e}")

@click.command()
@click.argument('csv_file')
@click.option('--expected-gbps', type=float, help='Expected throughput in Gbps')
def main(csv_file, expected_gbps):
    """Analyze network counter CSV data"""
    analyze_network_counters(csv_file, expected_gbps)

if __name__ == "__main__":
    main()