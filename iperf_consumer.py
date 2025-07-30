#!/usr/bin/env python3
import subprocess
import json
import time
import click
import os
from statistics import mean

class IperfConsumer:
    def __init__(self, bind_ip, port, interface):
        self.bind_ip = bind_ip
        self.port = port
        self.interface = interface
        
    def run_test(self, duration):
        # Start monitoring
        csv_file = f"network_consumer_{int(time.time())}.csv"
        monitor = subprocess.Popen(['python3', 'network_throughput_monitor.py', 
                                   '-i', self.interface, '-d', str(duration + 5), '-o', csv_file])
        
        # Start server
        results_file = f"iperf_server_{int(time.time())}.json"
        server = subprocess.Popen(['iperf3', '--server', '--bind', self.bind_ip, 
                                  '--port', str(self.port), '--json', '--one-time', 
                                  '--logfile', results_file])
        
        print(f"Server: {self.bind_ip}:{self.port} | Monitoring: {csv_file}")
        
        # Wait for test
        server.wait()
        time.sleep(1)
        
        # Read results
        with open(results_file, 'r') as f:
            result = json.load(f)
        
        self.analyze(result, csv_file)
        
        # Cleanup
        monitor.terminate()
        return result
    
    def analyze(self, result, csv_file):
        print("\n=== Results ===")
        
        recv = result['end']['sum_received']
        print(f"Received: {recv['bytes']/1024/1024:.1f} MB - {recv['bits_per_second']/1e9:.2f} Gbps")
        
        # Network analysis
        gbps = recv['bits_per_second'] / 1e9
        rate_hz = int((gbps * 1e9) / (1024 * 8))
        subprocess.run(['python3', 'analyze_network.py', csv_file, '-r', str(rate_hz), '-s', '1024'])
        
        print(f"Data: {csv_file}")


@click.command()
@click.option('--bind-ip', default='0.0.0.0')
@click.option('--port', default=5201)
@click.option('-t', default=60)
@click.option('-i', '--interface', default='lo0')
def main(bind_ip, port, t, interface):
    consumer = IperfConsumer(bind_ip, port, interface)
    consumer.run_test(t)

if __name__ == "__main__":
    main()