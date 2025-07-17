#!/usr/bin/env python3
"""
Pub-Sub Consumer with 4-Timestamp NTP Method
Version 1.0
"""

import time
import json
import zmq
import click

class PubSubConsumer:
    def __init__(self, node_id, producer_data_port=5555, viz_port=5556, producer_ip="localhost", control_port=5557):
        self.node_id = node_id
        self.producer_data_port = producer_data_port
        self.viz_port = viz_port
        self.producer_ip = producer_ip
        self.context = zmq.Context()
        self.control_port = control_port
        
    def timestamp_us(self):
        return int(time.time() * 1_000_000)
    
    def run_consumer(self, duration_sec=60, processing_time_ms=0.4):
        sub_socket = self.context.socket(zmq.SUB)
        sub_socket.connect(f"tcp://{self.producer_ip}:{self.producer_data_port}")
        sub_socket.setsockopt(zmq.SUBSCRIBE, b"request")
        
        pub_socket = self.context.socket(zmq.PUB)
        pub_socket.bind(f"tcp://*:{self.viz_port}")
        
        print(f"Consumer: subscribing to {self.producer_data_port}, publishing on {self.viz_port}")
        time.sleep(1)  # Allow pub-sub connections to establish
        
        # Send start control message
        control_socket = self.context.socket(zmq.REQ)
        control_socket.connect(f"tcp://{self.producer_ip}:{self.control_port}")
        control_socket.send_string("start")
        control_socket.recv_string()
        control_socket.close()
        print("Consumer: start signal sent, waiting for messages...")
        
        start_time = time.time()
        received_count = 0
        
        while time.time() - start_time < duration_sec:
            try:
                topic, data = sub_socket.recv_multipart(zmq.NOBLOCK)
                request = json.loads(data.decode())
                
                if request['type'] == 'request':
                    t2 = self.timestamp_us()
                    time.sleep(processing_time_ms / 1000)  # configurable processing time in ms
                    t3 = self.timestamp_us()
                    
                    response = {
                        'type': 'response', 'msg_id': request['msg_id'], 'seq_n': request['seq_n'],
                        't2': t2, 't3': t3, 'original_size': request['message_size'], 'status': 'processed'
                    }
                    
                    pub_socket.send_multipart([b"response", json.dumps(response).encode()])
                    received_count += 1
                    
                    if received_count % 100 == 0:
                        print(f"Consumer: processed {received_count}")
                
            except Exception as e:
                print(f"Consumer error: {e}")
                break
        
        sub_socket.close()
        pub_socket.close()
        print(f"Consumer: processed {received_count} messages")

@click.command()
@click.option('-t', default=60, help='Duration seconds')
@click.option('--producer-ip', default='localhost', help='Visualization IP address')
@click.option('--producer-data-port', default=5555, help='Server publisher port')
@click.option('--client-pub-port', default=5556, help='Client publisher port')
@click.option('--control-port', default=5557, help='Control channel port')
@click.option('--processing-time-ms', default=0.4, help='Processing time in milliseconds')
def main(t, producer_ip, producer_data_port, viz_port, control_port, processing_time_ms):
    """Pub-Sub Consumer with 4-timestamp NTP method"""
    
    consumer_node = PubSubConsumer("consumer", producer_data_port, viz_port, producer_ip, control_port)
    consumer_node.run_consumer(t + 10, processing_time_ms)

if __name__ == "__main__":
    main()