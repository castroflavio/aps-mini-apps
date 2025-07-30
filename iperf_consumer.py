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
        
    def run_consumer(self, duration, total_bytes=None):
        print(f"Consumer (client): connecting to {self.server_ip}:{self.port}")
        
        # Start monitoring
        csv_file = f"network_consumer_{int(time.time())}.csv"
        monitor = subprocess.Popen(['python3', 'network_throughput_monitor.py', 
                                   '-i', self.interface, '-d', str(duration + 5), '-o', csv_file])
        
        # Wait a moment for producer to be ready
        time.sleep(1)
        
        # Build iperf client command
        cmd = ['iperf3', '-c', self.server_ip, '-p', str(self.port), '-J']
        
        if total_bytes:
            cmd.extend(['-n', str(total_bytes)])
            print(f"Transferring {total_bytes/1024/1024:.1f} MB")
        else:
            cmd.extend(['-t', str(duration)])
            print(f"Running {duration}s test")
        
        # Run iperf client
        result = subprocess.run(cmd, capture_output=True, text=True)
        iperf_result = json.loads(result.stdout)
        
        self.analyze_results(iperf_result, csv_file)
        
        # Cleanup
        monitor.terminate()
        return iperf_result
    
    def analyze_results(self, result, csv_file):
        print("\n=== Consumer Results ===")
        
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
@click.option('--server-ip', default='localhost')
@click.option('--port', default=5201)
@click.option('-t', default=30)
@click.option('--bytes', default=0)
@click.option('-s', '--msg-size', default=1024)
@click.option('-i', '--interface', default='lo0')
def main(server_ip, port, t, bytes, msg_size, interface):
    """Iperf Consumer (Client) - connects to producer"""
    
    if bytes == 0:
        bytes = int(t * 1000 * msg_size)  # 1000 Hz equivalent
        print(f"Using {bytes/1024/1024:.1f} MB ({t}s @ 1000 Hz, {msg_size}B msgs)")
    
    consumer = IperfConsumer(server_ip, port, interface)
    consumer.run_consumer(t, bytes if bytes > 0 else None)

if __name__ == "__main__":
    main()