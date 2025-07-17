#!/usr/bin/env python3
"""
Network Throughput Monitor for Debian/Linux
Monitors interface counters using /proc/net/dev
"""

import time
import csv
import click
from datetime import datetime

class NetworkMonitor:
    def __init__(self, interface):
        self.interface = interface
        self.prev_tx_bytes = 0
        self.prev_rx_bytes = 0
        self.prev_tx_packets = 0
        self.prev_rx_packets = 0
        self.prev_time = 0
        self.first_sample = True
        
    def get_interface_stats(self):
        """Get interface statistics from /proc/net/dev"""
        try:
            with open('/proc/net/dev', 'r') as f:
                for line in f:
                    if f'{self.interface}:' in line:
                        parts = line.split()
                        rx_bytes = int(parts[1])
                        rx_packets = int(parts[2])
                        tx_bytes = int(parts[9])
                        tx_packets = int(parts[10])
                        return tx_bytes, rx_bytes, tx_packets, rx_packets
        except (FileNotFoundError, ValueError, IndexError):
            pass
        return None, None, None, None
    
    def calculate_throughput(self):
        """Calculate throughput metrics"""
        current_time = time.time()
        tx_bytes, rx_bytes, tx_packets, rx_packets = self.get_interface_stats()
        
        if any(x is None for x in [tx_bytes, rx_bytes, tx_packets, rx_packets]):
            return None
            
        if self.first_sample:
            self.prev_tx_bytes = tx_bytes
            self.prev_rx_bytes = rx_bytes
            self.prev_tx_packets = tx_packets
            self.prev_rx_packets = rx_packets
            self.prev_time = current_time
            self.first_sample = False
            return None
        
        time_diff = current_time - self.prev_time
        if time_diff <= 0:
            return None
            
        # Calculate throughput
        tx_bps = (tx_bytes - self.prev_tx_bytes) * 8 / time_diff
        rx_bps = (rx_bytes - self.prev_rx_bytes) * 8 / time_diff
        tx_pps = (tx_packets - self.prev_tx_packets) / time_diff
        rx_pps = (rx_packets - self.prev_rx_packets) / time_diff
        
        # Update previous values
        self.prev_tx_bytes = tx_bytes
        self.prev_rx_bytes = rx_bytes
        self.prev_tx_packets = tx_packets
        self.prev_rx_packets = rx_packets
        self.prev_time = current_time
        
        return {
            'timestamp': datetime.now().isoformat(),
            'tx_throughput_bps': max(0, tx_bps),
            'rx_throughput_bps': max(0, rx_bps),
            'tx_throughput_pps': max(0, tx_pps),
            'rx_throughput_pps': max(0, rx_pps),
            'tx_throughput_mbps': max(0, tx_bps) / 1_000_000,
            'rx_throughput_mbps': max(0, rx_bps) / 1_000_000
        }

@click.command()
@click.option('-i', '--interface', required=True, help='Network interface to monitor')
@click.option('-d', '--duration', default=60, help='Duration in seconds')
@click.option('-o', '--output', default='network_throughput.csv', help='Output CSV file')
@click.option('-v', '--verbose', is_flag=True, help='Verbose output')
def main(interface, duration, output, verbose):
    """Network throughput monitor for Debian/Linux"""
    
    monitor = NetworkMonitor(interface)
    
    print(f"Monitoring interface {interface} for {duration} seconds...")
    
    with open(output, 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'tx_throughput_bps', 'rx_throughput_bps', 
                     'tx_throughput_pps', 'rx_throughput_pps', 
                     'tx_throughput_mbps', 'rx_throughput_mbps']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        start_time = time.time()
        sample_count = 0
        
        while time.time() - start_time < duration:
            metrics = monitor.calculate_throughput()
            
            if metrics:
                writer.writerow(metrics)
                csvfile.flush()
                sample_count += 1
                
                if verbose:
                    print(f"TX: {metrics['tx_throughput_mbps']:.2f} Mbps, "
                          f"RX: {metrics['rx_throughput_mbps']:.2f} Mbps")
            
            time.sleep(1.0)
    
    print(f"Monitoring complete. Collected {sample_count} samples.")

if __name__ == "__main__":
    main()