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
        
    def run_producer(self, duration):
        # Start monitoring
        csv_file = f"network_producer_{int(time.time())}.csv"
        monitor = subprocess.Popen(['python3', 'network_throughput_monitor.py', 
                                   '-i', self.interface, '-d', str(duration + 10), '-o', csv_file])
        
        # Start iperf server
        results_file = f"iperf_producer_{int(time.time())}.json"
        server = subprocess.Popen(['iperf3', '--server', '--bind', self.bind_ip, 
                                  '--port', str(self.port), '--json', '--one-time', 
                                  '--logfile', results_file])
        
        print(f"Producer (server): {self.bind_ip}:{self.port}")
        print(f"Monitoring: {csv_file}")
        print("Waiting for consumer connection...")
        
        # Wait for test completion
        server.wait()
        time.sleep(1)
        
        # Read and analyze results
        with open(results_file, 'r') as f:
            result = json.load(f)
        
        self.analyze_results(result, csv_file)
        
        # Cleanup
        monitor.terminate()
        return result
    
    def analyze_results(self, result, csv_file):
        print("\n=== Producer Results ===")
        
        sent = result['end']['sum_sent']
        recv = result['end']['sum_received']
        
        print(f"Sent: {sent['bytes']/1024/1024:.1f} MB - {sent['bits_per_second']/1e9:.2f} Gbps")
        print(f"Received: {recv['bytes']/1024/1024:.1f} MB - {recv['bits_per_second']/1e9:.2f} Gbps")
        
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
@click.option('-t', default=60)
@click.option('-i', '--interface', default='lo0')
def main(bind_ip, port, t, interface):
    """Iperf Producer (Server) - waits for connections"""
    producer = IperfProducer(bind_ip, port, interface)
    producer.run_producer(t)

if __name__ == "__main__":
    main()