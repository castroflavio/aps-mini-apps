#!/usr/bin/env python3
"""
Pub-Sub Producer with 4-Timestamp NTP Method
Version 2.8
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
            tx_gbps = tx_bps / 1e9  # Convert to Gbps
            print(f"Network: {len(streaming_data)} samples, TX: {np.mean(tx_gbps):.2f}±{np.std(tx_gbps):.2f} Gbps, PPS: {np.mean(tx_pps):.0f}±{np.std(tx_pps):.0f}")
    
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
                    # Use bytes for cached (immutable) - will need concatenation
                messages = [(b'request|seq_n=' + str(i).encode() + b'|msg_id=' + str(i).encode() + b'|t1=' + b'0'*20 + b'|data=' + data_payload, str(i), len(b'request|seq_n=' + str(i).encode() + b'|msg_id=' + str(i).encode() + b'|t1=')) for i in range(total_messages)]
                cached_data = np.array(messages, dtype=object)
                np.save(cache_file, cached_data)
                return cached_data
        else:
            # Use bytearray for in-place timestamp updates (critical for large messages)
            messages = []
            for i in range(total_messages):
                prefix = b'request|seq_n=' + str(i).encode() + b'|msg_id=' + str(i).encode() + b'|t1='
                message_data = bytearray(prefix + b'0'*20 + b'|data=' + data_payload)
                timestamp_offset = len(prefix)
                messages.append((message_data, str(i), timestamp_offset))
            return messages
    
    def run_producer(self, rate_hz=10, duration_sec=60, msg_size=1024, interface='lo0', use_cache=False):
        control_socket = self.context.socket(zmq.REP)
        control_socket.bind(f"tcp://*:{self.control_port}")
        pub_socket = self.context.socket(zmq.PUB)
        pub_socket.set_hwm(1000)  # Prevent memory explosion at high rates
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
        sent_count = 0
        
        # Simplified instrumentation - focus on message prep bottleneck
        time_string_concat = 0
        
        while seq_n < total_messages and ((time.time() - start_time) < duration_sec):
            message_data, msg_id, timestamp_offset = prepared_messages[seq_n]
            
            t1 = self.timestamp_us()
            timestamp_bytes = str(t1).encode().ljust(20, b'0')[:20]
            
            # Time only the critical message preparation step
            t_start = time.perf_counter()
            if isinstance(message_data, bytearray):
                message_data[timestamp_offset:timestamp_offset+20] = timestamp_bytes
                final_message = bytes(message_data)
            else:
                final_message = message_data[:timestamp_offset] + timestamp_bytes + message_data[timestamp_offset+20:]
            time_string_concat += time.perf_counter() - t_start
            
            self.pending_messages[msg_id] = {'seq_n': seq_n, 't1': t1, 'msg_size': msg_size}
            
            try:
                pub_socket.send_multipart([b"request", final_message], flags=zmq.NOBLOCK, copy=False)
                sent_count += 1
            except zmq.Again:
                skipped += 1
                self.pending_messages.pop(msg_id)
                if seq_n % 100 == 0:
                    print(f"ZMQ queue full! Skipped {skipped}/{seq_n} messages ({100*skipped/seq_n:.1f}%)")
            
            seq_n += 1
            
            next_send_time += interval
            sleep_time = next_send_time - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                # Critical: warn if falling behind at high rates
                if seq_n % 1000 == 0:
                    behind_ms = -sleep_time * 1000
                    current_success_rate = sent_count / (time.time() - start_time)
                    print(f"WARNING: {behind_ms:.1f}ms behind schedule, actual rate: {current_success_rate:.1f}Hz (target: {rate_hz}Hz)")
        
        elapsed_time = time.time() - start_time
        attempt_rate = seq_n / elapsed_time
        success_rate = sent_count / elapsed_time
        total_bytes_sent = sent_count * msg_size
        throughput_gbps = (total_bytes_sent * 8) / (elapsed_time * 1e9)
        
        print(f"\n=== Producer Summary ===")
        print(f"Messages attempted: {seq_n:,}")
        print(f"Messages sent: {sent_count:,} ({skipped:,} skipped = {100*skipped/seq_n:.1f}%)")
        print(f"Duration: {elapsed_time:.1f}s")
        print(f"Attempt rate: {attempt_rate:.1f} Hz (target: {rate_hz} Hz)")
        print(f"Success rate: {success_rate:.1f} Hz")
        print(f"Throughput: {throughput_gbps:.2f} Gbps")
        
        # Simplified analysis - focus on message prep bottleneck
        avg_concat_us = (time_string_concat / seq_n) * 1e6
        max_rate_hz = 1e6 / avg_concat_us if avg_concat_us > 0 else float('inf')
        target_interval_us = 1e6 / rate_hz  # Target time between messages
        prep_overhead_pct = (avg_concat_us / target_interval_us) * 100
        
        print(f"\n=== Message Prep Analysis ===")
        print(f"Avg msg prep time: {avg_concat_us:.0f}µs {'[in-place]' if any(isinstance(m[0], bytearray) for m in prepared_messages[:1]) else '[concat]'}")
        print(f"Target interval:   {target_interval_us:.0f}µs ({rate_hz}Hz)")
        print(f"Prep overhead:     {prep_overhead_pct:.1f}% of target interval")
        print(f"Max sustainable:   {max_rate_hz:.0f} Hz (based on msg prep alone)")
        
        # Simple warnings
        if prep_overhead_pct > 50:
            print(f"⚠️  Message prep takes {prep_overhead_pct:.0f}% of target interval - rate too high for message size")
        elif prep_overhead_pct > 20:
            print(f"⚠️  Message prep takes {prep_overhead_pct:.0f}% of target interval - approaching limits")
        else:
            print(f"✅ Message prep overhead is acceptable ({prep_overhead_pct:.0f}%)")
        
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
