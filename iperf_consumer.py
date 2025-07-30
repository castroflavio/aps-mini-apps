#!/usr/bin/env python3
"""
Iperf Consumer (Client) - connects to producer and runs test
"""

import subprocess
import json
import time
import click
import os

class IperfConsumer:
    def __init__(self, server_ip, port, interface):
        self.server_ip = server_ip
        self.port = port
        self.interface = interface
        
    def run_consumer(self, total_size_gb):
        total_bytes = int(total_size_gb * 1024**3)  # Convert GB to bytes
        estimated_duration = max(30, int(total_size_gb * 10))  # ~10 seconds per GB
        
        print(f"Consumer (client): connecting to {self.server_ip}:{self.port}")
        print(f"Transferring {total_size_gb:.1f} GB ({total_bytes:,} bytes)")
        
        # Start monitoring
        csv_file = f"network_consumer_{int(time.time())}.csv"
        monitor = subprocess.Popen(['python3', 'netmonitor.py', 
                                   '-i', self.interface, '-d', str(estimated_duration), '-o', csv_file])
        
        # Wait a moment for producer to be ready
        time.sleep(1)
        
        # Build iperf client command
        cmd = ['iperf316', '-c', self.server_ip, '-p', str(self.port), '-J', '-n', str(total_bytes)]
        print(f"Running: {' '.join(cmd)}")
        
        # Run iperf client
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Stop monitoring immediately after iperf completes
        print("Iperf test completed, stopping network monitor...")
        monitor.terminate()
        monitor.wait()  # Wait for clean shutdown
        
        # Give a moment for any buffering
        time.sleep(1)
        
        iperf_result = json.loads(result.stdout)
        
        self.analyze_results(iperf_result, csv_file, total_size_gb)
        
        return iperf_result
    
    def analyze_results(self, result, csv_file, expected_gb):
        print("\n=== Consumer Results ===")
        
        sent = result['end']['sum_sent']
        recv = result['end']['sum_received']
        
        # Use actual duration from iperf logs
        actual_duration = sent['seconds']
        sent_gb = sent['bytes'] / (1024**3)
        recv_gb = recv['bytes'] / (1024**3)
        
        # Calculate throughput using iperf's duration
        actual_gbps = (sent['bytes'] * 8) / (actual_duration * 1e9)
        
        print(f"Sent: {sent_gb:.2f} GB in {actual_duration:.1f}s")
        print(f"Received: {recv_gb:.2f} GB")
        print(f"Throughput: {actual_gbps:.2f} Gbps (from iperf duration)")
        print(f"Efficiency: {(sent_gb/expected_gb)*100:.1f}% of expected {expected_gb:.1f} GB")
        
        if 'retransmits' in sent and sent['retransmits'] > 0:
            print(f"Retransmits: {sent['retransmits']}")
        
        # Network analysis using actual throughput
        print(f"\n=== Network Interface Analysis ===")
        subprocess.run(['python3', 'analyze_netmonitor.py', csv_file, '--expected-gbps', str(actual_gbps)])
        
        print(f"Data: {csv_file}")

@click.command()
@click.option('--server-ip', default='localhost')
@click.option('--port', default=5201)
@click.option('--size', default=1.0, help='Total size in GB')
@click.option('-i', '--interface', default='lo0')
def main(server_ip, port, size, interface):
    """Iperf Consumer (Client) - connects to producer"""
    
    consumer = IperfConsumer(server_ip, port, interface)
    consumer.run_consumer(size)

if __name__ == "__main__":
    main()