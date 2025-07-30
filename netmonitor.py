#!/usr/bin/env python3
"""
Network Counter Monitor - samples network interface counters every second
"""

import time
import click
import csv
import psutil
from datetime import datetime

class NetworkMonitor:
    def __init__(self, interface, output_file):
        self.interface = interface
        self.output_file = output_file
        
    def get_interface_stats(self):
        """Get network interface statistics"""
        try:
            stats = psutil.net_io_counters(pernic=True)
            if self.interface in stats:
                iface_stats = stats[self.interface]
                return {
                    'timestamp': datetime.now().isoformat(),
                    'bytes_sent': iface_stats.bytes_sent,
                    'bytes_recv': iface_stats.bytes_recv,
                    'packets_sent': iface_stats.packets_sent,
                    'packets_recv': iface_stats.packets_recv,
                    'errin': iface_stats.errin,
                    'errout': iface_stats.errout,
                    'dropin': iface_stats.dropin,
                    'dropout': iface_stats.dropout
                }
            else:
                available_interfaces = list(stats.keys())
                raise Exception(f"Interface '{self.interface}' not found. Available: {available_interfaces}")
        except Exception as e:
            raise Exception(f"Error reading interface stats: {e}")
    
    def run_monitor(self, duration):
        """Monitor interface for specified duration, sampling every second"""
        print(f"Monitoring {self.interface} for {duration}s -> {self.output_file}")
        
        # Write CSV header
        fieldnames = ['timestamp', 'bytes_sent', 'bytes_recv', 'packets_sent', 'packets_recv', 
                     'errin', 'errout', 'dropin', 'dropout']
        
        sample_count = 0
        csvfile = None
        
        try:
            csvfile = open(self.output_file, 'w', newline='')
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            csvfile.flush()
            
            start_time = time.time()
            
            while time.time() - start_time < duration:
                try:
                    stats = self.get_interface_stats()
                    writer.writerow(stats)
                    csvfile.flush()  # Ensure data is written immediately
                    
                    sample_count += 1
                    if sample_count % 10 == 0:  # Progress update every 10 seconds
                        elapsed = time.time() - start_time
                        print(f"Sampled {sample_count} times ({elapsed:.1f}s elapsed)")
                    
                    time.sleep(1)
                    
                except KeyboardInterrupt:
                    print(f"\nInterrupted! Saved {sample_count} samples to {self.output_file}")
                    break
                except Exception as e:
                    print(f"Sampling error: {e}")
                    # Continue monitoring despite individual sample errors
                    time.sleep(1)
                    continue
            
            print(f"Monitoring complete: {sample_count} samples in {self.output_file}")
            
        except IOError as e:
            print(f"File error: {e}")
            return False
        except Exception as e:
            print(f"Monitoring failed: {e}")
            return False
        finally:
            if csvfile:
                csvfile.close()
            if sample_count > 0:
                print(f"✅ Data saved: {sample_count} samples")
            else:
                print("❌ No data collected")
        
        return sample_count > 0

@click.command()
@click.option('-i', '--interface', required=True, help='Network interface to monitor')
@click.option('-d', '--duration', default=60, help='Duration in seconds')
@click.option('-o', '--output', required=True, help='Output CSV file')
def main(interface, duration, output):
    """Network Counter Monitor - samples interface counters every second"""
    
    monitor = NetworkMonitor(interface, output)
    monitor.run_monitor(duration)

if __name__ == "__main__":
    main()