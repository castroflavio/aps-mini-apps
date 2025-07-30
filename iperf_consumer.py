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
        monitor = subprocess.Popen(['python3', 'network_throughput_monitor.py', 
                                   '-i', self.interface, '-d', str(estimated_duration), '-o', csv_file])
        
        # Wait a moment for producer to be ready
        time.sleep(1)
        
        # Build iperf client command
        cmd = ['iperf3', '-c', self.server_ip, '-p', str(self.port), '-J', '-n', str(total_bytes)]
        
        # Run iperf client
        result = subprocess.run(cmd, capture_output=True, text=True)
        iperf_result = json.loads(result.stdout)
        
        self.analyze_results(iperf_result, csv_file, total_size_gb)
        
        # Cleanup
        monitor.terminate()
        return iperf_result
    
    def analyze_results(self, result, csv_file, expected_gb):
        print("\n=== Consumer Results ===")
        
        sent = result['end']['sum_sent']
        recv = result['end']['sum_received']
        duration = sent['seconds']
        
        sent_gb = sent['bytes'] / (1024**3)
        recv_gb = recv['bytes'] / (1024**3)
        
        print(f"Sent: {sent_gb:.2f} GB in {duration:.1f}s - {sent['bits_per_second']/1e9:.2f} Gbps")
        print(f"Received: {recv_gb:.2f} GB - {recv['bits_per_second']/1e9:.2f} Gbps")
        print(f"Efficiency: {(sent_gb/expected_gb)*100:.1f}% of expected {expected_gb:.1f} GB")
        
        if 'retransmits' in sent and sent['retransmits'] > 0:
            print(f"Retransmits: {sent['retransmits']}")
        
        # Network analysis
        gbps = sent['bits_per_second'] / 1e9
        rate_hz = int((gbps * 1e9) / (1024 * 8))
        subprocess.run(['python3', 'analyze_network.py', csv_file, '-r', str(rate_hz), '-s', '1024'])
        
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