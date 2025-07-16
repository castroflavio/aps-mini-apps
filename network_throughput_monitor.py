#!/usr/bin/env python3
"""
Network Throughput Monitor - Measures interface counters and throughput
"""

import time
import csv
import psutil
import click
from datetime import datetime

@click.command()
@click.option('--interface', '-i', required=True, help='Network interface to monitor')
@click.option('--duration', '-d', default=60, help='Duration in seconds (default: 60)')
@click.option('--output', '-o', default='throughput.csv', help='Output CSV file (default: throughput.csv)')
def main(interface, duration, output):
    """Monitor network interface throughput and save to CSV"""
    
    # Check if interface exists
    if interface not in psutil.net_io_counters(pernic=True):
        print(f"Error: Interface '{interface}' not found")
        print(f"Available interfaces: {list(psutil.net_io_counters(pernic=True).keys())}")
        return
    
    print(f"Monitoring {interface} for {duration}s, saving to {output}")
    
    # CSV setup
    with open(output, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['timestamp', 'bytes_sent', 'bytes_recv', 'packets_sent', 'packets_recv', 
                        'tx_throughput_mbps', 'rx_throughput_mbps', 'tx_pps', 'rx_pps'])
        
        # Initial counters
        prev_stats = psutil.net_io_counters(pernic=True)[interface]
        prev_time = time.time()
        
        start_time = time.time()
        
        while time.time() - start_time < duration:
            time.sleep(0.1)
            
            current_stats = psutil.net_io_counters(pernic=True)[interface]
            current_time = time.time()
            time_diff = current_time - prev_time
            
            # Calculate throughput
            bytes_sent_diff = current_stats.bytes_sent - prev_stats.bytes_sent
            bytes_recv_diff = current_stats.bytes_recv - prev_stats.bytes_recv
            packets_sent_diff = current_stats.packets_sent - prev_stats.packets_sent
            packets_recv_diff = current_stats.packets_recv - prev_stats.packets_recv
            
            tx_throughput_mbps = (bytes_sent_diff * 8) / (time_diff * 1_000_000)  # Mbps
            rx_throughput_mbps = (bytes_recv_diff * 8) / (time_diff * 1_000_000)  # Mbps
            tx_pps = packets_sent_diff / time_diff  # packets per second
            rx_pps = packets_recv_diff / time_diff  # packets per second
            
            # Write to CSV
            writer.writerow([
                datetime.now().isoformat(),
                current_stats.bytes_sent,
                current_stats.bytes_recv,
                current_stats.packets_sent,
                current_stats.packets_recv,
                f"{tx_throughput_mbps:.2f}",
                f"{rx_throughput_mbps:.2f}",
                f"{tx_pps:.0f}",
                f"{rx_pps:.0f}"
            ])
            
            # Console output
            print(f"TX: {tx_throughput_mbps:.2f} Mbps ({tx_pps:.0f} pps), "
                  f"RX: {rx_throughput_mbps:.2f} Mbps ({rx_pps:.0f} pps)")
            
            # Update previous values
            prev_stats = current_stats
            prev_time = current_time
    
    print(f"Monitoring complete. Results saved to {output}")

if __name__ == "__main__":
    main()