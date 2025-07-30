#!/usr/bin/env python3
"""
Iperf Producer with Network Monitoring
"""

import subprocess
import json
import time
import click
import os
from statistics import mean

class IperfProducer:
    def __init__(self, server_ip, server_port, interface, interval=1.0):
        self.server_ip = server_ip
        self.server_port = server_port
        self.interface = interface
        self.interval = interval
        
    def run_iperf(self, duration, total_bytes=None):
        cmd = ['iperf3', '-c', self.server_ip, '-p', str(self.server_port), '-J', '-i', str(self.interval)]
        
        if total_bytes:
            cmd.extend(['-n', str(total_bytes)])
            print(f"Transferring {total_bytes/1024/1024:.1f} MB to {self.server_ip}:{self.server_port}")
        else:
            cmd.extend(['-t', str(duration)])
            print(f"Running {duration}s test to {self.server_ip}:{self.server_port}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=duration + 30)
        if result.returncode != 0:
            raise Exception(f"iperf3 failed: {result.stderr}")
        return json.loads(result.stdout)
    
    def analyze_results(self, result):
        print("\n=== Iperf Results ===")
        
        end = result.get('end', {})
        if 'sum_sent' in end:
            sent = end['sum_sent']
            recv = end['sum_received']
            
            print(f"Sent: {sent['bytes']/1024/1024:.1f} MB in {sent['seconds']:.1f}s")
            print(f"Rate: {sent['bits_per_second']/1e9:.2f} Gbps")
            
            if 'retransmits' in sent and sent['retransmits'] > 0:
                print(f"Retransmits: {sent['retransmits']}")
        
        # Interval stability
        intervals = result.get('intervals', [])
        if len(intervals) > 2:
            rates = [i['sum']['bits_per_second']/1e9 for i in intervals]
            avg, min_rate, max_rate = mean(rates), min(rates), max(rates)
            variation = ((max_rate - min_rate) / avg) * 100
            
            print(f"Stability: {variation:.1f}% variation (avg {avg:.2f} Gbps)")
            print("✅ Stable" if variation < 5 else "⚠️ Variable" if variation < 15 else "❌ Unstable")
        
        return result
    
    def start_monitoring(self, duration):
        csv_file = f"network_iperf_{self.interface}_{int(time.time())}.csv"
        cmd = ['python3', 'network_throughput_monitor.py', '-i', self.interface, '-d', str(duration + 5), '-o', csv_file]
        
        process = subprocess.Popen(cmd)
        print(f"Monitoring: {csv_file}")
        time.sleep(0.5)
        return process, csv_file
    
    def analyze_network(self, csv_file, gbps):
        rate_hz = int((gbps * 1e9) / (1024 * 8))  # Assume 1KB messages
        cmd = ['python3', 'analyze_network.py', csv_file, '-r', str(rate_hz), '-s', '1024']
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(result.stdout)
        else:
            print(f"Network analysis failed: {result.stderr}")
    
    def run(self, duration, total_bytes=None):
        print(f"Iperf Producer: {self.server_ip}:{self.server_port} | {self.interface}")
        
        monitor_proc, csv_file = self.start_monitoring(duration)
        
        try:
            result = self.run_iperf(duration, total_bytes)
            analyzed = self.analyze_results(result)
            
            time.sleep(2)  # Let monitoring finish
            
            # Network analysis
            end = analyzed.get('end', {})
            if 'sum_sent' in end:
                gbps = end['sum_sent']['bits_per_second'] / 1e9
                print(f"\n=== Network Analysis ===")
                self.analyze_network(csv_file, gbps)
            
            print(f"\nData: {csv_file}")
            return analyzed
            
        finally:
            if monitor_proc:
                monitor_proc.terminate()
                try: monitor_proc.wait(timeout=5)
                except: monitor_proc.kill()

@click.command()
@click.option('--server-ip', default='localhost', help='Server IP')
@click.option('--server-port', default=5201, help='Server port')
@click.option('-t', default=30, help='Duration (seconds)')
@click.option('--bytes', default=0, help='Total bytes (0 = use duration)')
@click.option('-s', '--msg-size', default=1024, help='Message size for comparison')
@click.option('-i', '--interface', default='lo0', help='Network interface')
@click.option('--interval', default=1.0, help='Report interval')
def main(server_ip, server_port, t, bytes, msg_size, interface, interval):
    """Iperf Producer with Network Monitoring"""
    
    if bytes == 0:
        bytes = int(t * 1000 * msg_size)  # 1000 Hz equivalent
        print(f"Using {bytes/1024/1024:.1f} MB ({t}s @ 1000 Hz, {msg_size}B msgs)")
    
    producer = IperfProducer(server_ip, server_port, interface, interval)
    
    try:
        result = producer.run(t, bytes if bytes > 0 else None)
        
        # Save results
        results_file = f"iperf_producer_{int(time.time())}.json"
        with open(results_file, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"Saved: {results_file}")
        
    except Exception as e:
        print(f"Error: {e}")
        exit(1)

if __name__ == "__main__":
    main()
