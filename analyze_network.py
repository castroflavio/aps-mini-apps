#!/usr/bin/env python3
"""
Network throughput analysis script
Extracts and analyzes network CSV data from network_throughput_monitor.py
"""

import pandas as pd
import numpy as np
import argparse
import sys
import os

def analyze_network_csv(csv_file, rate_hz, msg_size):
    """
    Analyze network throughput CSV data
    
    Args:
        csv_file: Path to CSV file from network_throughput_monitor.py
        rate_hz: Expected message rate in Hz
        msg_size: Message size in bytes
    """
    try:
        # Check if file exists
        if not os.path.exists(csv_file):
            print(f"Network CSV file not found: {csv_file}")
            return
        
        # Read CSV data
        df = pd.read_csv(csv_file)
        
        # Check for required columns
        if 'tx_throughput_bps' not in df.columns or 'tx_throughput_pps' not in df.columns:
            print("Network: CSV missing required columns (tx_throughput_bps, tx_throughput_pps)")
            return
        
        # Filter for relevant throughput data (80% of expected rate)
        expected_bps = rate_hz * msg_size * 8  # Convert to bits per second
        threshold = expected_bps * 0.8
        data = df[df['tx_throughput_bps'] > threshold]
        
        if len(data) > 1:
            gbps = data['tx_throughput_bps'].values / 1e9
            pps = data['tx_throughput_pps'].values
            
            print(f"Network: {len(data)} samples, TX: {np.mean(gbps):.2f}±{np.std(gbps):.2f} Gbps, PPS: {np.mean(pps):.0f}±{np.std(pps):.0f}")
            
            # Additional statistics if available
            if len(data) > 10:
                print(f"  Peak: {np.max(gbps):.2f} Gbps, Min: {np.min(gbps):.2f} Gbps")
                print(f"  P95: {np.percentile(gbps, 95):.2f} Gbps, P99: {np.percentile(gbps, 99):.2f} Gbps")
        else:
            print(f"Network: insufficient data samples ({len(data)} found above {threshold/1e9:.2f} Gbps threshold)")
            if len(df) > 0:
                max_throughput = df['tx_throughput_bps'].max() / 1e9
                print(f"  Maximum observed: {max_throughput:.2f} Gbps")
    
    except Exception as e:
        print(f"Network analysis error: {e}")

def main():
    parser = argparse.ArgumentParser(description='Analyze network throughput CSV data')
    parser.add_argument('csv_file', help='CSV file from network_throughput_monitor.py')
    parser.add_argument('-r', '--rate', type=float, required=True, help='Expected message rate in Hz')
    parser.add_argument('-s', '--size', type=int, required=True, help='Message size in bytes')
    
    args = parser.parse_args()
    
    analyze_network_csv(args.csv_file, args.rate, args.size)

if __name__ == "__main__":
    main()