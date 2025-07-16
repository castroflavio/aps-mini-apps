#!/usr/bin/env python3
"""
Pub-Sub Synchronization Measurement with 4-Timestamp NTP Method
"""

import time
import json
import numpy as np
import zmq
import threading
import uuid
import click

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
        self.control_port = 5557  # Control channel port
        
    def timestamp_us(self):
        return int(time.time() * 1_000_000)
    
    def export_measurements(self, filename: str):
        data = [{
            'seq_n': m.seq_n,
            'msg_id': m.msg_id,
            'timestamps': [m.t1, m.t2, m.t3, m.t4],
            'clock_offset_us': m.clock_offset_us,
            'network_delay_us': m.network_delay_us,
            'processing_delay_us': m.processing_delay_us,
            'total_delay_us': m.total_delay_us,
            'message_size': m.message_size
        } for m in self.measurements]
        
        with open(filename, 'w') as f:
            json.dump({'node_id': self.node_id, 'measurements': data, 'count': len(data)}, f, indent=2)
    
    def run_server(self, rate_hz=10, duration_sec=60, message_sizes=[1024, 1048576, 10485760]):
        # Setup control channel
        control_socket = self.context.socket(zmq.REP)
        control_socket.bind(f"tcp://*:{self.control_port}")
        
        pub_socket = self.context.socket(zmq.PUB)
        pub_socket.bind(f"tcp://*:{self.server_pub_port}")
        
        sub_socket = self.context.socket(zmq.SUB)
        sub_socket.connect(f"tcp://{self.server_ip}:{self.client_pub_port}")
        sub_socket.setsockopt(zmq.SUBSCRIBE, b"response")
        
        print(f"Server: {rate_hz}Hz, {duration_sec}s, sizes={message_sizes}")
        
        threading.Thread(target=self._server_response_listener, args=(sub_socket,), daemon=True).start()
        
        # Wait for client ready signal
        print("Server: waiting for client ready...")
        control_socket.recv_string()
        control_socket.send_string("ready")
        control_socket.close()
        print("Server: client ready, starting test...")
        
        total_messages = int(rate_hz * duration_sec)
        print(f"Generating {total_messages} messages")
        
        start_time = time.time()
        seq_n = 0
        
        while time.time() - start_time < duration_sec:
            msg_size = message_sizes[seq_n % len(message_sizes)]
            msg_id = str(uuid.uuid4())
            t1 = self.timestamp_us()
            
            message = {
                'type': 'request', 'seq_n': seq_n, 'msg_id': msg_id, 't1': t1,
                'message_size': msg_size, 'data': 'x' * msg_size
            }
            
            self.pending_messages[msg_id] = {'seq_n': seq_n, 't1': t1, 'message_size': msg_size}
            pub_socket.send_multipart([b"request", json.dumps(message).encode()])
            
            if seq_n % 100 == 0:
                print(f"Server: published seq={seq_n}, size={msg_size}B")
            
            seq_n += 1
            time.sleep(1.0 / rate_hz)
        
        time.sleep(5)
        pub_socket.close()
        sub_socket.close()
        
        self.export_measurements("server_pubsub_results.json")
        print(f"Server: completed {len(self.measurements)}/{total_messages} measurements")
    
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
                    
                    if len(self.measurements) % 100 == 0:
                        print(f"Server: {len(self.measurements)} responses, "
                              f"offset={measurement.clock_offset_us}μs")
                
            except zmq.Again:
                time.sleep(0.001)
            except Exception as e:
                print(f"Server error: {e}")
                break
    
    def run_client(self, duration_sec=60):
        sub_socket = self.context.socket(zmq.SUB)
        sub_socket.connect(f"tcp://{self.server_ip}:{self.server_pub_port}")
        sub_socket.setsockopt(zmq.SUBSCRIBE, b"request")
        
        pub_socket = self.context.socket(zmq.PUB)
        pub_socket.bind(f"tcp://*:{self.client_pub_port}")
        
        print(f"Client: subscribing to {self.server_pub_port}, publishing on {self.client_pub_port}")
        time.sleep(1)  # Allow pub-sub connections to establish
        
        # Signal server that client is ready
        control_socket = self.context.socket(zmq.REQ)
        control_socket.connect(f"tcp://{self.server_ip}:{self.control_port}")
        control_socket.send_string("ready")
        control_socket.recv_string()
        control_socket.close()
        print("Client: ready signal sent, waiting for messages...")
        
        start_time = time.time()
        received_count = 0
        
        while time.time() - start_time < duration_sec:
            try:
                topic, data = sub_socket.recv_multipart(zmq.NOBLOCK)
                request = json.loads(data.decode())
                
                if request['type'] == 'request':
                    t2 = self.timestamp_us()
                    time.sleep(0.0004)  # 400μs processing
                    t3 = self.timestamp_us()
                    
                    response = {
                        'type': 'response', 'msg_id': request['msg_id'], 'seq_n': request['seq_n'],
                        't2': t2, 't3': t3, 'original_size': request['message_size'], 'status': 'processed'
                    }
                    
                    pub_socket.send_multipart([b"response", json.dumps(response).encode()])
                    received_count += 1
                    
                    if received_count % 100 == 0:
                        print(f"Client: processed {received_count}")
                
            except zmq.Again:
                time.sleep(0.001)
            except Exception as e:
                print(f"Client error: {e}")
                break
        
        sub_socket.close()
        pub_socket.close()
        print(f"Client: processed {received_count} messages")

def run_test(rate_hz, duration_sec, message_sizes, server_pub_port=5555, client_pub_port=5556):
    server_node = PubSubSyncNode("server", server_pub_port, client_pub_port)
    client_node = PubSubSyncNode("client", server_pub_port, client_pub_port)
    
    threading.Thread(target=client_node.run_client, args=(duration_sec + 10,), daemon=True).start()
    time.sleep(1)  # Reduced since control channel handles sync
    server_node.run_server(rate_hz, duration_sec, message_sizes)
    
    if server_node.measurements:
        measurements = server_node.measurements
        offsets = [m.clock_offset_us for m in measurements]
        delays = [m.network_delay_us for m in measurements]
        processing = [m.processing_delay_us for m in measurements]
        total_delays = [m.total_delay_us for m in measurements]
        
        # Group by message size
        size_groups = {}
        for m in measurements:
            size = m.message_size
            if size not in size_groups:
                size_groups[size] = {'offsets': [], 'delays': [], 'processing': [], 'total': []}
            size_groups[size]['offsets'].append(m.clock_offset_us)
            size_groups[size]['delays'].append(m.network_delay_us)
            size_groups[size]['processing'].append(m.processing_delay_us)
            size_groups[size]['total'].append(m.total_delay_us)
        
        print(f"\n=== Pub-Sub Sync Results (F1.md Format) ===")
        print(f"Rate: {rate_hz} Hz, Duration: {duration_sec}s, Total: {len(measurements)}")
        print(f"Overall: offset={np.mean(offsets):.1f}μs, delay={np.mean(delays):.1f}μs, "
              f"processing={np.mean(processing):.1f}μs, total={np.mean(total_delays):.1f}μs")
        
        print(f"\nPer-Message-Size Analysis:")
        for size, metrics in size_groups.items():
            size_str = f"{size//1048576}MB" if size >= 1048576 else f"{size//1024}KB" if size >= 1024 else f"{size}B"
            print(f"  {size_str} ({len(metrics['offsets'])} samples): "
                  f"offset={np.mean(metrics['offsets']):.1f}μs, "
                  f"delay={np.mean(metrics['delays']):.1f}μs, "
                  f"processing={np.mean(metrics['processing']):.1f}μs, "
                  f"total={np.mean(metrics['total']):.1f}μs")

@click.command()
@click.argument('role', type=click.Choice(['server', 'client', 'test']))
@click.option('--rate-hz', default=10.0, help='Message rate Hz')
@click.option('--duration', default=60, help='Duration seconds')
@click.option('--message-sizes', default="1024,1048576,10485760", help='Message sizes (comma-separated)')
@click.option('--server-ip', default='localhost', help='Server IP address')
@click.option('--server-pub-port', default=5555, help='Server publisher port')
@click.option('--client-pub-port', default=5556, help='Client publisher port')
def main(role, rate_hz, duration, message_sizes, server_ip, server_pub_port, client_pub_port):
    """Pub-Sub synchronization measurement with 4-timestamp NTP method"""
    
    sizes = [int(s) for s in message_sizes.split(',')]
    
    if role == 'test':
        run_test(rate_hz, duration, sizes, server_pub_port, client_pub_port)
        
    elif role == 'server':
        print(f"=== Server Mode ===")
        print(f"Rate: {rate_hz} Hz, Duration: {duration}s, Sizes: {sizes}")
        
        server_node = PubSubSyncNode("server", server_pub_port, client_pub_port)
        server_node.server_ip = server_ip
        server_node.run_server(rate_hz, duration + 30, sizes)
        
        if server_node.measurements:
            measurements = server_node.measurements
            offsets = [m.clock_offset_us for m in measurements]
            delays = [m.network_delay_us for m in measurements]
            processing = [m.processing_delay_us for m in measurements]
            total_delays = [m.total_delay_us for m in measurements]
            
            print(f"\n=== Server Results ===")
            print(f"Completed: {len(measurements)} measurements")
            print(f"Offset: {np.mean(offsets):.1f}μs, Delay: {np.mean(delays):.1f}μs, "
                  f"Processing: {np.mean(processing):.1f}μs, Total: {np.mean(total_delays):.1f}μs")
        
    elif role == 'client':
        print(f"=== Client Mode ===")
        print(f"Connecting to {server_ip}, Duration: {duration}s")
        
        client_node = PubSubSyncNode("client", server_pub_port, client_pub_port)
        client_node.server_ip = server_ip
        client_node.run_client(duration + 10)

if __name__ == "__main__":
    main()