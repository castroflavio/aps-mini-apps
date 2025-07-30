#!/usr/bin/env python3
"""
Analyze network counter data from netmonitor.py
"""

import pandas as pd
import numpy as np
import click
from datetime import datetime

def find_stream_boundaries(df, threshold_pct=0.15):
    """
    Find stream start and end using 15% threshold of maximum throughput
    Returns: (start_idx, end_idx) or (None, None) if no stream found
    """
    # Use TX throughput for boundary detection
    tx_gbps = df['tx_gbps'].values
    
    if len(tx_gbps) == 0 or tx_gbps.max() == 0:
        return None, None
    
    max_throughput = tx_gbps.max()
    threshold = max_throughput * threshold_pct
    
    # Find first point above threshold (stream start)
    above_threshold = tx_gbps > threshold
    
    if not above_threshold.any():
        return None, None
    
    # Find first and last indices above threshold
    start_idx = np.argmax(above_threshold)  # First True index
    end_idx = len(above_threshold) - 1 - np.argmax(above_threshold[::-1])  # Last True index
    
    return start_idx, end_idx

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
        
        # Find stream boundaries using 15% threshold
        stream_start_idx, stream_end_idx = find_stream_boundaries(valid_data)
        stream_data = valid_data.iloc[stream_start_idx:stream_end_idx+1] if stream_start_idx is not None else valid_data
        
        # Calculate durations
        total_monitoring_duration = df['elapsed'].iloc[-1]
        total_sent_gb = (df['bytes_sent'].iloc[-1] - df['bytes_sent'].iloc[0]) / 1e9
        total_recv_gb = (df['bytes_recv'].iloc[-1] - df['bytes_recv'].iloc[0]) / 1e9
        
        # Determine which duration and data to report
        if stream_start_idx is not None:
            # Use stream boundaries for primary reporting
            stream_start_time = valid_data.iloc[stream_start_idx]['elapsed']
            stream_end_time = valid_data.iloc[stream_end_idx]['elapsed']
            active_duration = stream_end_time - stream_start_time
            stream_sent_gb = (valid_data.iloc[stream_end_idx]['bytes_sent'] - valid_data.iloc[stream_start_idx]['bytes_sent']) / 1e9
            stream_recv_gb = (valid_data.iloc[stream_end_idx]['bytes_recv'] - valid_data.iloc[stream_start_idx]['bytes_recv']) / 1e9
            
            # Report stream-based statistics as primary
            print(f"\n=== Network Counter Analysis ===")
            print(f"Stream Duration: {active_duration:.1f}s | Active Samples: {stream_end_idx - stream_start_idx + 1}")
            print(f"Stream TX: {stream_sent_gb:.2f} GB | Stream RX: {stream_recv_gb:.2f} GB")
            print(f"Monitoring: {total_monitoring_duration:.1f}s total ({(active_duration/total_monitoring_duration)*100:.1f}% active)")
            
            # Set primary duration for throughput calculations
            primary_duration = active_duration
            primary_sent_gb = stream_sent_gb
            primary_recv_gb = stream_recv_gb
        else:
            # Fallback to total duration if no stream detected
            print(f"\n=== Network Counter Analysis ===")
            print(f"Duration: {total_monitoring_duration:.1f}s | Samples: {len(df)}")
            print(f"Total TX: {total_sent_gb:.2f} GB | Total RX: {total_recv_gb:.2f} GB")
            print("No clear stream boundaries detected (using total duration)")
            
            primary_duration = total_monitoring_duration
            primary_sent_gb = total_sent_gb
            primary_recv_gb = total_recv_gb
        
        # Throughput statistics using stream data
        tx_nonzero = stream_data[stream_data['tx_gbps'] > 0]['tx_gbps']
        rx_nonzero = stream_data[stream_data['rx_gbps'] > 0]['rx_gbps']
        
        if len(tx_nonzero) > 0:
            tx_mean = tx_nonzero.mean()
            tx_max = tx_nonzero.max()
            tx_std = tx_nonzero.std()
            
            print(f"\nTX Throughput: Avg={tx_mean:.2f} Gbps, Max={tx_max:.2f} Gbps, Std={tx_std:.2f}")
            
            if expected_gbps:
                efficiency = (tx_mean / expected_gbps) * 100
                print(f"TX Efficiency: {efficiency:.1f}% of expected {expected_gbps:.2f} Gbps")
                
            # Calculate overall stream throughput for comparison
            if stream_start_idx is not None:
                overall_stream_gbps = (primary_sent_gb * 8) / primary_duration
                print(f"Overall Stream Rate: {overall_stream_gbps:.2f} Gbps over {primary_duration:.1f}s")
        
        if len(rx_nonzero) > 0:
            rx_mean = rx_nonzero.mean()
            rx_max = rx_nonzero.max()
            rx_std = rx_nonzero.std()
            
            print(f"RX Throughput: Avg={rx_mean:.2f} Gbps, Max={rx_max:.2f} Gbps, Std={rx_std:.2f}")
        
        # Peak activity periods (from stream data)
        peak_tx_idx = stream_data['tx_gbps'].idxmax() if len(tx_nonzero) > 0 else None
        peak_rx_idx = stream_data['rx_gbps'].idxmax() if len(rx_nonzero) > 0 else None
        
        if peak_tx_idx is not None:
            peak_time = stream_data.loc[peak_tx_idx, 'elapsed']
            peak_value = stream_data.loc[peak_tx_idx, 'tx_gbps']
            print(f"Peak TX: {peak_value:.2f} Gbps at {peak_time:.1f}s")
        
        if peak_rx_idx is not None:
            peak_time = stream_data.loc[peak_rx_idx, 'elapsed']
            peak_value = stream_data.loc[peak_rx_idx, 'rx_gbps']
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