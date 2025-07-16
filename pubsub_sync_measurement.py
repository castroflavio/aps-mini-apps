#!/usr/bin/env python3
"""
Integrated Pub-Sub Sync Measurement with Network Monitoring
"""

import time
import json
import numpy as np
import zmq
import threading
import uuid
import click
import subprocess
import csv
import os
from datetime import datetime

class PubSubMeasurement:
    def __init__(self, seq_n, msg_id, t1, t2, t3, t4, message_size):
        self.seq_n = seq_n
        self.msg_id = msg_id
        self.t1, self.t2, self.t3, self.t4 = t1, t2, t3, t4
        self.message_size = message_size
        self.clock_offset_us = ((t2 - t1) + (t3 - t4)) // 2
        self.network_delay_us = ((t4 - t1) - (t3 - t2)) // 2
        self.processing_delay_us = t3 - t2
        self.total_delay_us = t4 - t1

class PubSubSyncNode:
    def __init__(self, node_id: str, server_pub_port=5555, client_pub_port=5556):
        self.node_id = node_id
        self.server_pub_port = server_pub_port
        self.client_pub_port = client_pub_port
        self.server_ip = 'localhost'
        self.context = zmq.Context()
        self.measurements = []
        self.pending_messages = {}
        self.control_port = 5557
        self.streaming_started = False
        
    def timestamp_us(self):
        return int(time.time() * 1_000_000)
    
    def run_server(self, rate_hz, duration_sec, message_sizes):
        control_socket = self.context.socket(zmq.REP)
        control_socket.bind(f"tcp://*:{self.control_port}")
        
        pub_socket = self.context.socket(zmq.PUB)
        pub_socket.bind(f"tcp://*:{self.server_pub_port}")
        
        sub_socket = self.context.socket(zmq.SUB)
        sub_socket.connect(f"tcp://{self.server_ip}:{self.client_pub_port}")
        sub_socket.setsockopt(zmq.SUBSCRIBE, b"response")
        
        threading.Thread(target=self._server_response_listener, args=(sub_socket,), daemon=True).start()
        
        print("Server: waiting for client ready...")
        control_socket.recv_string()
        control_socket.send_string("ready")
        control_socket.close()
        
        total_messages = int(rate_hz * duration_sec)
        print(f"Server: starting test, {total_messages} messages at {rate_hz}Hz")
        
        self.streaming_started = True
        start_time = time.time()
        seq_n = 0
        
        while seq_n < total_messages and time.time() - start_time < duration_sec:
            msg_size = message_sizes[seq_n % len(message_sizes)]
            msg_id = str(uuid.uuid4())
            t1 = self.timestamp_us()
            
            message = {
                'type': 'request', 'seq_n': seq_n, 'msg_id': msg_id, 't1': t1,
                'message_size': msg_size, 'data': 'x' * msg_size
            }
            
            self.pending_messages[msg_id] = {'seq_n': seq_n, 't1': t1, 'message_size': msg_size}
            pub_socket.send_multipart([b"request", json.dumps(message).encode()])
            
            seq_n += 1
            time.sleep(1.0 / rate_hz)
        
        print(f"Server: published {seq_n} messages in {time.time() - start_time:.1f}s")
        time.sleep(2)
        pub_socket.close()
        sub_socket.close()
    
    def _server_response_listener(self, sub_socket):
        while True:
            try:
                topic, data = sub_socket.recv_multipart(zmq.NOBLOCK)
                response = json.loads(data.decode())
                
                if response['type'] == 'response' and response['msg_id'] in self.pending_messages:
                    t4 = self.timestamp_us()
                    pending = self.pending_messages.pop(response['msg_id'])
                    
                    measurement = PubSubMeasurement(
                        pending['seq_n'], response['msg_id'], pending['t1'], 
                        response['t2'], response['t3'], t4, pending['message_size']
                    )
                    self.measurements.append(measurement)
                
            except zmq.Again:
                time.sleep(0.001)
            except Exception:
                break
    
    def run_client(self, duration_sec):
        sub_socket = self.context.socket(zmq.SUB)
        sub_socket.connect(f"tcp://{self.server_ip}:{self.server_pub_port}")
        sub_socket.setsockopt(zmq.SUBSCRIBE, b"request")
        
        pub_socket = self.context.socket(zmq.PUB)
        pub_socket.bind(f"tcp://*:{self.client_pub_port}")
        
        time.sleep(0.5)
        
        control_socket = self.context.socket(zmq.REQ)
        control_socket.connect(f"tcp://{self.server_ip}:{self.control_port}")
        control_socket.send_string("ready")
        control_socket.recv_string()
        control_socket.close()
        
        start_time = time.time()
        received_count = 0
        
        while time.time() - start_time < duration_sec:
            try:
                topic, data = sub_socket.recv_multipart(zmq.NOBLOCK)
                request = json.loads(data.decode())
                
                if request['type'] == 'request':
                    t2 = self.timestamp_us()
                    time.sleep(0.0004)
                    t3 = self.timestamp_us()
                    
                    response = {
                        'type': 'response', 'msg_id': request['msg_id'], 'seq_n': request['seq_n'],
                        't2': t2, 't3': t3, 'status': 'processed'
                    }
                    
                    pub_socket.send_multipart([b"response", json.dumps(response).encode()])
                    received_count += 1
                
            except zmq.Again:
                time.sleep(0.001)
            except Exception:
                break
        
        sub_socket.close()
        pub_socket.close()
        print(f"Client: processed {received_count} messages")

def analyze_throughput(csv_file, expected_rate_mbps, start_time, duration):
    """Analyze throughput data and find streaming boundaries"""
    data = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append({
                'timestamp': datetime.fromisoformat(row['timestamp']),
                'tx_mbps': float(row['tx_throughput_mbps']),
                'rx_mbps': float(row['rx_throughput_mbps'])
            })
    
    if not data:
        print("Error: No throughput data found")
        return
    
    # Find streaming boundaries (>10% of expected rate)
    threshold_mbps = expected_rate_mbps * 0.1
    streaming_periods = []
    
    for i, sample in enumerate(data):
        if sample['tx_mbps'] > threshold_mbps:
            streaming_periods.append(i)
    
    if not streaming_periods:
        print(f"Error: No streaming detected above {threshold_mbps:.2f} Mbps threshold")
        return
    
    start_idx = streaming_periods[0]
    end_idx = streaming_periods[-1]
    streaming_data = data[start_idx:end_idx+1]
    
    if len(streaming_data) < 5:
        print("Error: Streaming period too short for analysis")
        return
    
    # Calculate statistics
    tx_rates = [d['tx_mbps'] for d in streaming_data]
    rx_rates = [d['rx_mbps'] for d in streaming_data]
    
    print(f"\n=== Network Throughput Analysis ===")
    print(f"Streaming period: {len(streaming_data)} samples ({len(streaming_data)*0.1:.1f}s)")
    print(f"Expected rate: {expected_rate_mbps:.2f} Mbps")
    print(f"TX - Avg: {np.mean(tx_rates):.2f} Mbps, P25: {np.percentile(tx_rates, 25):.2f} Mbps, P75: {np.percentile(tx_rates, 75):.2f} Mbps")
    print(f"RX - Avg: {np.mean(rx_rates):.2f} Mbps, P25: {np.percentile(rx_rates, 25):.2f} Mbps, P75: {np.percentile(rx_rates, 75):.2f} Mbps")

@click.command()
@click.option('--interface', '-i', required=True, help='Network interface to monitor')
@click.option('--rate-hz', default=10.0, help='Message rate Hz')
@click.option('--duration', default=60, help='Duration seconds')
@click.option('--message-sizes', default="1024,1048576,10485760", help='Message sizes (comma-separated)')
def main(interface, rate_hz, duration, message_sizes):
    """Integrated pub-sub sync measurement with network monitoring"""
    
    sizes = [int(s) for s in message_sizes.split(',')]
    avg_msg_size = np.mean(sizes)
    expected_rate_mbps = (rate_hz * avg_msg_size * 8) / 1_000_000  # Convert to Mbps
    
    print(f"=== Integrated Sync & Network Test ===")
    print(f"Rate: {rate_hz} Hz, Duration: {duration}s, Interface: {interface}")
    print(f"Expected throughput: {expected_rate_mbps:.2f} Mbps")
    
    # Start network monitoring
    csv_file = f"network_{interface}_{int(time.time())}.csv"
    monitor_proc = subprocess.Popen([
        'python', 'network_throughput_monitor.py',
        '-i', interface, '-d', str(duration + 5), '-o', csv_file
    ])
    
    time.sleep(1)  # Let monitor start
    
    # Run pub-sub test
    server_node = PubSubSyncNode("server")
    client_node = PubSubSyncNode("client")
    
    threading.Thread(target=client_node.run_client, args=(duration + 5,), daemon=True).start()
    
    # Wait for streaming to start (with timeout)
    start_time = time.time()
    time.sleep(0.5)
    
    # Check if streaming started within timeout
    timeout_start = time.time()
    while not server_node.streaming_started and time.time() - timeout_start < 3:
        time.sleep(0.1)
    
    if not server_node.streaming_started:
        print("Error: Streaming failed to start within 3 seconds")
        monitor_proc.terminate()
        return
    
    server_node.run_server(rate_hz, duration, sizes)
    
    # Wait for monitor to finish
    monitor_proc.wait()
    
    # Analyze results
    if server_node.measurements:
        measurements = server_node.measurements
        offsets = [m.clock_offset_us for m in measurements]
        delays = [m.network_delay_us for m in measurements]
        
        print(f"\n=== Sync Measurement Results ===")
        print(f"Measurements: {len(measurements)}")
        print(f"Clock offset: {np.mean(offsets):.1f} μs")
        print(f"Network delay: {np.mean(delays):.1f} μs")
    
    # Analyze throughput
    analyze_throughput(csv_file, expected_rate_mbps, start_time, duration)
    
    print(f"\nNetwork data saved to: {csv_file}")

if __name__ == "__main__":
    main()