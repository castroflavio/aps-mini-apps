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
        # Start monitoring (estimate duration: 5s buffer + ~10s per GB)
        estimated_duration = 5 + max(30, int(total_size_gb * 10))
        csv_file = f"network_producer_{int(time.time())}.csv"
        monitor = subprocess.Popen(['python3', 'netmonitor.py', 
                                   '-i', self.interface, '-d', str(estimated_duration), '-o', csv_file])
        
        # Start iperf server
        results_file = f"iperf_producer_{int(time.time())}.json"
        server_cmd = ['iperf316', '--server', '--bind', self.bind_ip, 
                     '--port', str(self.port), '--json', '-1', 
                     '--logfile', results_file]
        print(f"Running: {' '.join(server_cmd)}")
        server = subprocess.Popen(server_cmd)
        
        print(f"Producer (server): {self.bind_ip}:{self.port}")
        print(f"Expecting {total_size_gb:.1f} GB transfer")
        print(f"Monitoring: {csv_file}")
        print("Waiting for consumer connection...")
        
        # Wait for test completion with timeout
        timeout_seconds = estimated_duration
        try:
            server.wait(timeout=timeout_seconds)
            print("Test completed, waiting for results file...")
        except subprocess.TimeoutExpired:
            print(f"Server timeout after {timeout_seconds}s, terminating...")
            server.terminate()
            server.wait()
        
        # Give iperf time to write the results file
        time.sleep(3)
        
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
@click.option('--bind-ip', default='0.0.0.0')
@click.option('--port', default=5201)
@click.option('--size', default=1.0, help='Expected size in GB (for monitoring duration)')
@click.option('-i', '--interface', default='lo0')
def main(bind_ip, port, size, interface):
    """Iperf Producer (Server) - waits for connections"""
    producer = IperfProducer(bind_ip, port, interface)
    producer.run_producer(size)

if __name__ == "__main__":
    main()
