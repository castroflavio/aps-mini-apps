#!/usr/bin/env python3
"""
Iperf Producer (Server) - starts iperf server, monitoring, and waits
"""

import subprocess
import json
import time
import click
import os

class IperfProducer:
    def __init__(self, bind_ip, port, interface):
        self.bind_ip = bind_ip
        self.port = port
        self.interface = interface
        
    def run_producer(self, total_size_gb):
        # Start monitoring (estimate duration based on size)
        estimated_duration = max(30, int(total_size_gb * 10))  # ~10 seconds per GB
        csv_file = f"network_producer_{int(time.time())}.csv"
        monitor = subprocess.Popen(['python3', 'network_throughput_monitor.py', 
                                   '-i', self.interface, '-d', str(estimated_duration), '-o', csv_file])
        
        # Start iperf server
        results_file = f"iperf_producer_{int(time.time())}.json"
        server = subprocess.Popen(['iperf3', '--server', '--bind', self.bind_ip, 
                                  '--port', str(self.port), '--json', '--one-time', 
                                  '--logfile', results_file])
        
        print(f"Producer (server): {self.bind_ip}:{self.port}")
        print(f"Expecting {total_size_gb:.1f} GB transfer")
        print(f"Monitoring: {csv_file}")
        print("Waiting for consumer connection...")
        
        # Wait for test completion
        server.wait()
        time.sleep(1)
        
        # Read and analyze results
        with open(results_file, 'r') as f:
            result = json.load(f)
        
        self.analyze_results(result, csv_file, total_size_gb)
        
        # Cleanup
        monitor.terminate()
        return result
    
    def analyze_results(self, result, csv_file, expected_gb):
        print("\n=== Producer Results ===")
        
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
@click.option('--bind-ip', default='0.0.0.0')
@click.option('--port', default=5201)
@click.option('--size', default=1.0, help='Total size in GB')
@click.option('-i', '--interface', default='lo0')
def main(bind_ip, port, size, interface):
    """Iperf Producer (Server) - waits for connections"""
    producer = IperfProducer(bind_ip, port, interface)
    producer.run_producer(size)

if __name__ == "__main__":
    main()