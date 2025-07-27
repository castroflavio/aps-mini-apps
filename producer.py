#!/usr/bin/env python3
"""
Pub-Sub Producer with 4-Timestamp NTP Method
Version 2.1
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
        if self.measurements:
            pd.DataFrame(self.measurements).to_csv(filename, index=False)
    
    def analyze_network_csv(self, csv_file, rate_hz, msg_size):
        df = pd.read_csv(csv_file)
        streaming_data = df[df['tx_throughput_bps'] > rate_hz * msg_size * 0.8]
        if len(streaming_data) > 1:
            tx_bps, tx_pps = streaming_data['tx_throughput_bps'].values, streaming_data['tx_throughput_pps'].values
            print(f"Network: {len(streaming_data)} samples, TX BPS: {np.mean(tx_bps):.0f}±{np.std(tx_bps):.0f}, TX PPS: {np.mean(tx_pps):.0f}±{np.std(tx_pps):.0f}")
    
    def _prepare_messages(self, total_messages, msg_size, rate_hz, use_cache=False):
        """Prepare messages for sending - either cached or pre-generated"""
        data_payload = b'x' * msg_size
        timestamp_width = 20  # Fixed-width timestamp field for O(1) updates
        
        if use_cache:
            # DAQ-style caching approach
            cache_file = f"producer_cache_{rate_hz}hz_{total_messages}msg_{msg_size}b.npy"
            try:
                return np.load(cache_file, allow_pickle=True)
            except FileNotFoundError:
                messages = [(b'request|seq_n=' + str(i).encode() + b'|msg_id=' + str(i).encode() + b'|t1=' + b'0'*20 + b'|data=' + data_payload, str(i), len(b'request|seq_n=' + str(i).encode() + b'|msg_id=' + str(i).encode() + b'|t1=')) for i in range(total_messages)]
                cached_data = np.array(messages, dtype=object)
                np.save(cache_file, cached_data)
                return cached_data
        else:
            return [(bytearray(b'request|seq_n=' + str(i).encode() + b'|msg_id=' + str(i).encode() + b'|t1=' + b'0'*20 + b'|data=' + data_payload), str(i), len(b'request|seq_n=' + str(i).encode() + b'|msg_id=' + str(i).encode() + b'|t1=')) for i in range(total_messages)]
    
    def run_producer(self, rate_hz=10, duration_sec=60, msg_size=1024, interface='lo0', use_cache=False):
        control_socket = self.context.socket(zmq.REP)
        control_socket.bind(f"tcp://*:{self.control_port}")
        pub_socket = self.context.socket(zmq.PUB)
        pub_socket.bind(f"tcp://*:{self.data_port}")
        
        network_csv = f"network_{interface}_{int(time.time())}.csv"
        monitor_proc = subprocess.Popen(['python', 'network_throughput_monitor.py', '-i', interface, '-d', str(duration_sec + 10), '-o', network_csv])
        time.sleep(0.2)

        control_socket.recv_string()
        control_socket.send_string("start")
        control_socket.close()
        
        viz_socket = self.context.socket(zmq.SUB)
        viz_socket.connect(f"tcp://{self.viz_ip}:{self.viz_port}")
        viz_socket.setsockopt(zmq.SUBSCRIBE, b"response")
        threading.Thread(target=self._vizualization_listener, args=(viz_socket,), daemon=True).start()
        
        total_messages = int(rate_hz * duration_sec)
        self.streaming_started = True
        start_time = time.time()
        seq_n = 0
        interval = 1.0 / rate_hz
        next_send_time = time.time()
        prepared_messages = self._prepare_messages(total_messages, msg_size, rate_hz, use_cache)
        skipped = 0
        
        while seq_n < total_messages and ((time.time() - start_time) < duration_sec):
            message_data, msg_id, timestamp_offset = prepared_messages[seq_n]
            t1 = self.timestamp_us()
            
            # Use fastest timestamp serialization (168ns vs 404ns for fixed-width)
            timestamp_bytes = str(t1).encode().ljust(20, b'0')[:20]
            
            # Use string concatenation (348ns) - faster than bytearray ops (404ns)
            final_message = message_data[:timestamp_offset] + timestamp_bytes + message_data[timestamp_offset+20:]
            
            self.pending_messages[msg_id] = {'seq_n': seq_n, 't1': t1, 'msg_size': msg_size}
            
            try:
                pub_socket.send_multipart([b"request", final_message], flags=zmq.NOBLOCK, copy=False)
            except zmq.Again:
                skipped += 1
                self.pending_messages.pop(msg_id)
            seq_n += 1
            
            next_send_time += interval
            sleep_time = next_send_time - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        time.sleep(5)
        pub_socket.close()
        viz_socket.close()
        monitor_proc.wait()
        return network_csv
    
    def _vizualization_listener(self, viz_socket):
        while True:
            try:
                topic, data = viz_socket.recv_multipart(zmq.NOBLOCK)
                response = json.loads(data.decode())
                
                if response['type'] == 'response' and response['msg_id'] in self.pending_messages:
                    t4 = self.timestamp_us()
                    pending = self.pending_messages.pop(response['msg_id'])
                    self.measurements.append(create_measurement(pending['seq_n'], response['msg_id'], pending['t1'], response['t2'], response['t3'], t4, pending['msg_size']))
            except zmq.Again:
                time.sleep(0.001)
            except Exception:
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
@click.option('--cache', is_flag=True, help='Use DAQ-style message caching')
def main(r, t, s, data_port, viz_port, control_port, viz_ip, interface, output, cache):
    """Pub-Sub Producer with 4-timestamp NTP method"""
    
    producer_node = PubSubProducer("producer", data_port, viz_port, viz_ip, control_port)
    network_csv = producer_node.run_producer(r, t, s, interface, cache)
    
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
