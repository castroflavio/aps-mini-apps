#!/usr/bin/env python3
"""
Pub-Sub Producer with 4-Timestamp NTP Method
Version 1.2
"""

import time
import json
import numpy as np
import zmq
import threading
import uuid
import click
import csv
import subprocess
import pandas as pd

def create_measurement(seq_n, msg_id, t1, t2, t3, t4, msg_size):
    return {
        'seq_n': seq_n,
        'msg_id': msg_id,
        't1': t1, 't2': t2, 't3': t3, 't4': t4,
        'message_size': msg_size,
        'clock_offset_ms': ((t2 - t1) + (t3 - t4)) // 2000,
        'network_delay_ms': ((t4 - t1) - (t3 - t2)) // 2000,
        'processing_delay_ms': (t3 - t2) // 1000,
        'total_delay_ms': (t4 - t1) // 1000
    }

class PubSubProducer:
    def __init__(self, node_id, data_port=5555, viz_port=5556, viz_ip="localhost", control_port=5557):
        self.node_id = node_id
        self.data_port = data_port
        self.viz_port = viz_port
        self.viz_ip = viz_ip
        self.context = zmq.Context()
        self.measurements = []
        self.pending_messages = {}
        self.control_port = control_port
        self.streaming_started = False
        
    def timestamp_us(self):
        return int(time.time() * 1_000_000)
    
    def export_measurements_csv(self, filename):
        with open(filename, 'w', newline='') as f:
            if self.measurements:
                fieldnames = ['seq_n', 'msg_id', 't1', 't2', 't3', 't4', 'message_size', 
                             'clock_offset_ms', 'network_delay_ms', 'processing_delay_ms', 'total_delay_ms']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.measurements)
    
    def analyze_network_csv(self, csv_file, rate_hz, msg_size):
        df = pd.read_csv(csv_file)
        expected_bps = rate_hz * msg_size * 8
        threshold = expected_bps * 0.1
        streaming_data = df[df['tx_throughput_bps'] > threshold]
        if len(streaming_data) < 2:
            print("Network analysis: Insufficient streaming data")
            return
        tx_bps = streaming_data['tx_throughput_bps'].values
        tx_pps = streaming_data['tx_throughput_pps'].values
        print(f"\n=== Network Analysis ===")
        print(f"Streaming samples: {len(streaming_data)}")
        print(f"TX BPS - Avg: {np.mean(tx_bps):.0f}, P50: {np.percentile(tx_bps, 50):.0f}, P99: {np.percentile(tx_bps, 99):.0f}")
        print(f"TX PPS - Avg: {np.mean(tx_pps):.0f}, P50: {np.percentile(tx_pps, 50):.0f}, P99: {np.percentile(tx_pps, 99):.0f}")
    
    def run_producer(self, rate_hz=10, duration_sec=60, msg_size=1024, interface='lo0'):
        # Setup control and data sockets
        control_socket = self.context.socket(zmq.REP)
        control_socket.bind(f"tcp://*:{self.control_port}")
        pub_socket = self.context.socket(zmq.PUB)
        pub_socket.bind(f"tcp://*:{self.data_port}")
        
        print(f"Producer: {rate_hz}Hz, {duration_sec}s, msg_size={msg_size}")
        
        # Start network monitoring
        network_csv = f"network_{interface}_{int(time.time())}.csv"
        monitor_proc = subprocess.Popen([
            'python', 'network_throughput_monitor.py',
            '-i', interface, '-d', str(duration_sec + 10), '-o', network_csv
        ])
        print(f"Network monitoring started: {network_csv}")
        time.sleep(0.2)  # Let monitor initialize

        # Wait for consumer start signal
        print("Producer: waiting for consumer start...")
        control_socket.recv_string()
        control_socket.send_string("start")
        control_socket.close()
        print("Producer: consumer start, starting test...")
        
        # Setup visualization socket after control message
        viz_socket = self.context.socket(zmq.SUB)
        viz_socket.connect(f"tcp://{self.viz_ip}:{self.viz_port}")
        viz_socket.setsockopt(zmq.SUBSCRIBE, b"response")
        threading.Thread(target=self._vizualization_listener, args=(viz_socket,), daemon=True).start()
        
        # Main streaming loop
        total_messages = int(rate_hz * duration_sec)
        print(f"Generating {total_messages} messages")
        self.streaming_started = True
        start_time = time.time()
        seq_n = 0
        
        # Compensating sleep strategy for precise timing
        interval = 1.0 / rate_hz
        next_send_time = time.time()
        
        # Pre-encode static parts for optimization
        data_payload = 'x' * msg_size
        
        while seq_n < total_messages and ((time.time() - start_time) < duration_sec):
            msg_id = f"{seq_n}"
            t1 = self.timestamp_us()
            message = {
                'type': 'request', 'seq_n': seq_n, 'msg_id': msg_id, 't1': t1,
                'msg_size': msg_size, 'data': data_payload
            }
            
            self.pending_messages[msg_id] = {'seq_n': seq_n, 't1': t1, 'msg_size': msg_size}
            pub_socket.send_multipart([b"request", json.dumps(message).encode()])
            
            if seq_n % rate_hz == 0:
                print(f"Producer: published seq={seq_n}, size={msg_size}B")
            seq_n += 1
            
            # Compensating sleep for precise rate control
            next_send_time += interval
            sleep_time = next_send_time - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                # Can't keep up with target rate!
                if seq_n % 100 == 0:
                    print(f"WARNING: {-sleep_time*1000:.1f}ms behind schedule")
        
        print(f"Producer: published {seq_n} messages in {time.time() - start_time:.1f}s")
        time.sleep(5) # Analysis timeout time at the producer
        pub_socket.close()
        viz_socket.close()
        
        # Wait for network monitoring to complete
        monitor_proc.wait()
        print(f"Network monitoring completed: {network_csv}")
        
        print(f"Producer: completed {len(self.measurements)}/{total_messages} measurements")
        return network_csv
    
    def _vizualization_listener(self, viz_socket):
        while True:
            try:
                topic, data = viz_socket.recv_multipart(zmq.NOBLOCK)
                response = json.loads(data.decode())
                
                if response['type'] == 'response' and response['msg_id'] in self.pending_messages:
                    t4 = self.timestamp_us()
                    pending = self.pending_messages.pop(response['msg_id'])
                    
                    measurement = create_measurement(
                        pending['seq_n'], response['msg_id'], pending['t1'], 
                        response['t2'], response['t3'], t4, pending['msg_size']
                    )
                    self.measurements.append(measurement)
                    
                    if len(self.measurements) % 100 == 0:
                        print(f"Producer: {len(self.measurements)} processed messages, "
                              f"offset={measurement['clock_offset_ms']}ms")
            except zmq.Again:
                time.sleep(0.001)
            except Exception as e:
                print(f"Producer error: {e}")
                break

@click.command()
@click.option('-r', default=10.0, help='Message rate Hz')
@click.option('-t', default=60, help='Duration seconds')
@click.option('-s', default=1024, help='Message size in bytes')
@click.option('--data-port', default=5555, help='Producer publisher port')
@click.option('--viz-port', default=5556, help='Client publisher port')
@click.option('--control-port', default=5557, help='Control channel port')
@click.option('--viz-ip', default='localhost', help='Visualization IP address')
@click.option('--interface', '-i', default='lo0', help='Network interface for monitoring')
@click.option('--output', default='producer_results.csv', help='Output CSV filename')
def main(r, t, s, data_port, viz_port, control_port, viz_ip, interface, output):
    """Pub-Sub Producer with 4-timestamp NTP method"""
    
    producer_node = PubSubProducer("producer", data_port, viz_port, viz_ip, control_port)
    network_csv = producer_node.run_producer(r, t, s, interface)
    
    if producer_node.measurements:
        offsets = [m['clock_offset_ms'] for m in producer_node.measurements]
        delays = [m['network_delay_ms'] for m in producer_node.measurements]
        processing = [m['processing_delay_ms'] for m in producer_node.measurements]
        total_delays = [m['total_delay_ms'] for m in producer_node.measurements]
        
        print(f"\n=== Producer Results ===")
        print(f"Completed: {len(producer_node.measurements)} measurements")
        print(f"Offset: {np.mean(offsets):.3f}ms (P99: {np.percentile(offsets, 99):.3f}ms)")
        print(f"Delay: {np.mean(delays):.3f}ms (P99: {np.percentile(delays, 99):.3f}ms)")
        print(f"Processing: {np.mean(processing):.3f}ms (P99: {np.percentile(processing, 99):.3f}ms)")
        print(f"Total: {np.mean(total_delays):.3f}ms (P99: {np.percentile(total_delays, 99):.3f}ms)")
        
        producer_node.export_measurements_csv(output)
        print(f"Results saved to: {output}")
    
    # Analyze network data
    producer_node.analyze_network_csv(network_csv, r, s)

if __name__ == "__main__":
    main()
