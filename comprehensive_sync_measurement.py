#!/usr/bin/env python3
"""
Comprehensive Streaming Synchronization Measurement

Combines NTP 4-timestamp method with streaming measurement to provide:
1. Clock offset calculation using NTP formulas
2. Network delay measurement 
3. Processing delay analysis
4. Streaming synchronization offset
5. Continuous measurement during data streaming
"""

import time
import json
import numpy as np
from collections import deque
import threading
import zmq
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class NTPTimestamp:
    """4-timestamp NTP measurement"""
    seq_n: int
    t1: int  # Client sends request
    t2: int  # Server receives request  
    t3: int  # Server sends response
    t4: int  # Client receives response
    message_size: int
    sender_id: str
    receiver_id: str

class StreamingNTPMeasurement:
    """Streaming synchronization measurement with NTP 4-timestamp method"""
    
    def __init__(self, node_id: str, clock_sync_enabled: bool = True, max_samples: int = 1000):
        self.node_id = node_id
        self.clock_sync_enabled = clock_sync_enabled
        self.max_samples = max_samples
        self.measurements = deque(maxlen=max_samples)
        self.baseline_timestamp = None
        self.stats_lock = threading.Lock()
        self.pending_requests = {}  # Track ongoing REQ/REP exchanges
        
    def get_timestamp_us(self) -> int:
        """Get current timestamp in microseconds"""
        return int(time.time() * 1_000_000)
    
    def start_request(self, seq_n: int, message_size: int, target_id: str) -> Dict:
        """Start NTP measurement for request (T1)"""
        t1 = self.get_timestamp_us()
        
        if not self.clock_sync_enabled and self.baseline_timestamp is None:
            self.baseline_timestamp = t1
            
        # Create message with T1 timestamp
        message = {
            'seq_n': seq_n,
            't1': t1,
            't2': 0,
            't3': 0,
            't4': 0,
            'message_size': message_size,
            'sender_id': self.node_id,
            'receiver_id': target_id,
            'measurement_id': str(uuid.uuid4())
        }
        
        # Store pending request
        self.pending_requests[seq_n] = message
        
        return message
    
    def receive_request(self, message: Dict) -> Dict:
        """Process received request and add T2 timestamp"""
        t2 = self.get_timestamp_us()
        
        if isinstance(message, dict) and 't1' in message:
            message['t2'] = t2
            message['receiver_id'] = self.node_id
            
        return message
    
    def send_response(self, message: Dict) -> Dict:
        """Send response with T3 timestamp"""
        t3 = self.get_timestamp_us()
        
        if isinstance(message, dict):
            message['t3'] = t3
            
        return message
    
    def receive_response(self, message: Dict) -> Optional[NTPTimestamp]:
        """Complete NTP measurement with T4 timestamp"""
        t4 = self.get_timestamp_us()
        
        if isinstance(message, dict) and all(k in message for k in ['t1', 't2', 't3']):
            message['t4'] = t4
            
            # Create NTP timestamp record
            ntp_ts = NTPTimestamp(
                seq_n=message.get('seq_n', 0),
                t1=message['t1'],
                t2=message['t2'],
                t3=message['t3'],
                t4=t4,
                message_size=message.get('message_size', 0),
                sender_id=message.get('sender_id', 'unknown'),
                receiver_id=self.node_id
            )
            
            # Store measurement
            with self.stats_lock:
                self.measurements.append(ntp_ts)
            
            # Remove from pending
            if ntp_ts.seq_n in self.pending_requests:
                del self.pending_requests[ntp_ts.seq_n]
            
            return ntp_ts
        
        return None
    
    def add_streaming_timestamp(self, seq_n: int, message_data: Dict, is_send: bool) -> int:
        """Add streaming timestamp for PUB/SUB pattern"""
        timestamp = self.get_timestamp_us()
        
        if not self.clock_sync_enabled and self.baseline_timestamp is None:
            self.baseline_timestamp = timestamp
            
        if is_send:
            message_data['stream_send_ts'] = timestamp
            message_data['stream_sender_id'] = self.node_id
        else:
            message_data['stream_recv_ts'] = timestamp
            message_data['stream_receiver_id'] = self.node_id
            
            # Calculate streaming offset
            if 'stream_send_ts' in message_data:
                send_ts = message_data['stream_send_ts']
                if self.clock_sync_enabled:
                    offset = timestamp - send_ts
                else:
                    # Clock-independent measurement
                    if self.baseline_timestamp is not None:
                        relative_send = send_ts - self.baseline_timestamp
                        relative_recv = timestamp - self.baseline_timestamp
                        offset = relative_recv - relative_send
                    else:
                        offset = 0
                
                # Store streaming measurement
                streaming_measurement = {
                    'seq_n': seq_n,
                    'stream_send_ts': send_ts,
                    'stream_recv_ts': timestamp,
                    'stream_offset_us': offset,
                    'sender_id': message_data.get('stream_sender_id', 'unknown'),
                    'receiver_id': self.node_id
                }
                
                with self.stats_lock:
                    # Store as special streaming measurement
                    if not hasattr(self, 'streaming_measurements'):
                        self.streaming_measurements = deque(maxlen=self.max_samples)
                    self.streaming_measurements.append(streaming_measurement)
        
        return timestamp
    
    def calculate_clock_offset(self, ntp_ts: NTPTimestamp) -> int:
        """Calculate clock offset using NTP formula"""
        return ((ntp_ts.t2 - ntp_ts.t1) + (ntp_ts.t3 - ntp_ts.t4)) // 2
    
    def calculate_network_delay(self, ntp_ts: NTPTimestamp) -> int:
        """Calculate network delay using NTP formula"""
        return ((ntp_ts.t4 - ntp_ts.t1) - (ntp_ts.t3 - ntp_ts.t2)) // 2
    
    def calculate_processing_delay(self, ntp_ts: NTPTimestamp) -> int:
        """Calculate processing delay"""
        return ntp_ts.t3 - ntp_ts.t2
    
    def calculate_total_delay(self, ntp_ts: NTPTimestamp) -> int:
        """Calculate total delay"""
        return ntp_ts.t4 - ntp_ts.t1
    
    def get_ntp_statistics(self) -> Optional[Dict]:
        """Get NTP timing statistics"""
        with self.stats_lock:
            if not self.measurements:
                return None
            
            clock_offsets = [self.calculate_clock_offset(m) for m in self.measurements]
            network_delays = [self.calculate_network_delay(m) for m in self.measurements]
            processing_delays = [self.calculate_processing_delay(m) for m in self.measurements]
            total_delays = [self.calculate_total_delay(m) for m in self.measurements]
            
            return {
                'node_id': self.node_id,
                'ntp_measurement_count': len(self.measurements),
                'clock_offset': {
                    'mean_us': np.mean(clock_offsets),
                    'std_dev_us': np.std(clock_offsets),
                    'p95_us': np.percentile(clock_offsets, 95)
                },
                'network_delay': {
                    'mean_us': np.mean(network_delays),
                    'std_dev_us': np.std(network_delays),
                    'p95_us': np.percentile(network_delays, 95)
                },
                'processing_delay': {
                    'mean_us': np.mean(processing_delays),
                    'std_dev_us': np.std(processing_delays),
                    'p95_us': np.percentile(processing_delays, 95)
                },
                'total_delay': {
                    'mean_us': np.mean(total_delays),
                    'std_dev_us': np.std(total_delays),
                    'p95_us': np.percentile(total_delays, 95)
                }
            }
    
    def get_streaming_statistics(self) -> Optional[Dict]:
        """Get streaming synchronization statistics"""
        with self.stats_lock:
            if not hasattr(self, 'streaming_measurements') or not self.streaming_measurements:
                return None
            
            offsets = [m['stream_offset_us'] for m in self.streaming_measurements]
            
            return {
                'node_id': self.node_id,
                'streaming_measurement_count': len(offsets),
                'synchronization_offset': {
                    'mean_us': np.mean(offsets),
                    'std_dev_us': np.std(offsets),
                    'min_us': np.min(offsets),
                    'max_us': np.max(offsets),
                    'p50_us': np.percentile(offsets, 50),
                    'p95_us': np.percentile(offsets, 95),
                    'p99_us': np.percentile(offsets, 99)
                }
            }
    
    def get_combined_statistics(self) -> Dict:
        """Get comprehensive statistics combining NTP and streaming measurements"""
        ntp_stats = self.get_ntp_statistics()
        streaming_stats = self.get_streaming_statistics()
        
        return {
            'node_id': self.node_id,
            'clock_sync_enabled': self.clock_sync_enabled,
            'ntp_analysis': ntp_stats,
            'streaming_analysis': streaming_stats,
            'timestamp': int(time.time() * 1_000_000)
        }
    
    def export_detailed_measurements(self, filename: str):
        """Export detailed measurements to JSON"""
        with self.stats_lock:
            # Convert NTP measurements to dict
            ntp_data = []
            for m in self.measurements:
                ntp_data.append({
                    'seq_n': m.seq_n,
                    'timestamps': [m.t1, m.t2, m.t3, m.t4],
                    'clock_offset_us': self.calculate_clock_offset(m),
                    'network_delay_us': self.calculate_network_delay(m),
                    'processing_delay_us': self.calculate_processing_delay(m),
                    'total_delay_us': self.calculate_total_delay(m),
                    'message_size': m.message_size,
                    'sender_id': m.sender_id,
                    'receiver_id': m.receiver_id
                })
            
            # Get streaming measurements
            streaming_data = []
            if hasattr(self, 'streaming_measurements'):
                streaming_data = list(self.streaming_measurements)
            
            # Combined export
            data = {
                'node_id': self.node_id,
                'clock_sync_enabled': self.clock_sync_enabled,
                'ntp_measurements': ntp_data,
                'streaming_measurements': streaming_data,
                'statistics': self.get_combined_statistics()
            }
            
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)


class ComprehensiveBenchmark:
    """Comprehensive benchmark supporting both NTP and streaming measurements"""
    
    def __init__(self, req_port=5555, pub_port=5556):
        self.req_port = req_port
        self.pub_port = pub_port
        self.server_ip = 'localhost'
        self.context = zmq.Context()
        
    def run_ntp_server(self, duration_sec=60):
        """Run NTP-style REQ/REP server"""
        responder = self.context.socket(zmq.REP)
        responder.bind(f"tcp://*:{self.req_port}")
        
        ntp_measurement = StreamingNTPMeasurement("ntp_server")
        
        print(f"NTP Server started on port {self.req_port}")
        
        start_time = time.time()
        while time.time() - start_time < duration_sec:
            try:
                # Receive request
                message = responder.recv_json(zmq.NOBLOCK)
                
                # Add T2 timestamp
                message = ntp_measurement.receive_request(message)
                
                # Simulate processing delay
                time.sleep(0.001)  # 1ms processing
                
                # Add T3 timestamp and send response
                response = ntp_measurement.send_response(message)
                responder.send_json(response)
                
            except zmq.Again:
                time.sleep(0.001)
        
        # Export results
        ntp_measurement.export_detailed_measurements("ntp_server_results.json")
        stats = ntp_measurement.get_ntp_statistics()
        if stats:
            print(f"NTP Server processed {stats['ntp_measurement_count']} requests")
        
        responder.close()
    
    def run_ntp_client(self, rate_hz=10, duration_sec=60, message_sizes=[1024, 1024*1024]):
        """Run NTP-style REQ/REP client"""
        requester = self.context.socket(zmq.REQ)
        requester.connect(f"tcp://{self.server_ip}:{self.req_port}")
        
        ntp_measurement = StreamingNTPMeasurement("ntp_client")
        
        print(f"NTP Client connected to port {self.req_port}")
        
        start_time = time.time()
        seq_n = 0
        
        while time.time() - start_time < duration_sec:
            for msg_size in message_sizes:
                # Start NTP measurement (T1)
                message = ntp_measurement.start_request(seq_n, msg_size, "ntp_server")
                message['data'] = 'x' * msg_size  # Simulate data
                
                # Send request
                requester.send_json(message)
                
                # Receive response and complete measurement (T4)
                response = requester.recv_json()
                ntp_ts = ntp_measurement.receive_response(response)
                
                if ntp_ts and seq_n % 100 == 0:
                    print(f"NTP Client seq {seq_n}: "
                          f"clock_offset={ntp_measurement.calculate_clock_offset(ntp_ts)}μs, "
                          f"network_delay={ntp_measurement.calculate_network_delay(ntp_ts)}μs, "
                          f"processing_delay={ntp_measurement.calculate_processing_delay(ntp_ts)}μs")
                
                seq_n += 1
                time.sleep(1.0 / rate_hz)
        
        # Export results
        ntp_measurement.export_detailed_measurements("ntp_client_results.json")
        stats = ntp_measurement.get_ntp_statistics()
        
        if stats:
            print(f"\nNTP Client Statistics:")
            print(f"  Requests: {stats['ntp_measurement_count']}")
            print(f"  Clock Offset: {stats['clock_offset']['mean_us']:.1f} ± {stats['clock_offset']['std_dev_us']:.1f} μs")
            print(f"  Network Delay: {stats['network_delay']['mean_us']:.1f} ± {stats['network_delay']['std_dev_us']:.1f} μs")
            print(f"  Processing Delay: {stats['processing_delay']['mean_us']:.1f} ± {stats['processing_delay']['std_dev_us']:.1f} μs")
            print(f"  Total Delay: {stats['total_delay']['mean_us']:.1f} ± {stats['total_delay']['std_dev_us']:.1f} μs")
        
        requester.close()
        return stats
    
    def run_streaming_publisher(self, rate_hz=10, duration_sec=60, message_sizes=[1024]):
        """Run streaming publisher with synchronization measurement"""
        publisher = self.context.socket(zmq.PUB)
        publisher.bind(f"tcp://*:{self.pub_port}")
        
        stream_measurement = StreamingNTPMeasurement("streaming_publisher")
        
        print(f"Streaming Publisher started on port {self.pub_port}")
        
        time.sleep(1)  # Allow subscribers to connect
        
        start_time = time.time()
        seq_n = 0
        
        while time.time() - start_time < duration_sec:
            for msg_size in message_sizes:
                # Create message
                message = {
                    'seq_n': seq_n,
                    'data': 'x' * msg_size,
                    'message_size': msg_size,
                    'type': 'streaming_data'
                }
                
                # Add streaming timestamp
                stream_measurement.add_streaming_timestamp(seq_n, message, is_send=True)
                
                # Send message
                publisher.send_json(message)
                
                seq_n += 1
                time.sleep(1.0 / rate_hz)
        
        stream_measurement.export_detailed_measurements("streaming_publisher_results.json")
        publisher.close()
    
    def run_streaming_subscriber(self, subscriber_id="streaming_subscriber", duration_sec=60):
        """Run streaming subscriber with synchronization measurement"""
        subscriber = self.context.socket(zmq.SUB)
        subscriber.connect(f"tcp://{self.server_ip}:{self.pub_port}")
        subscriber.setsockopt(zmq.SUBSCRIBE, b"")
        
        stream_measurement = StreamingNTPMeasurement(subscriber_id)
        
        print(f"Streaming Subscriber {subscriber_id} connected to port {self.pub_port}")
        
        start_time = time.time()
        received_count = 0
        
        while time.time() - start_time < duration_sec:
            try:
                # Receive message
                message = subscriber.recv_json(zmq.NOBLOCK)
                
                # Add receive timestamp
                stream_measurement.add_streaming_timestamp(message['seq_n'], message, is_send=False)
                
                received_count += 1
                if received_count % 100 == 0:
                    print(f"Streaming Subscriber {subscriber_id}: received {received_count} messages")
                
            except zmq.Again:
                time.sleep(0.001)
        
        # Export results
        stream_measurement.export_detailed_measurements(f"{subscriber_id}_results.json")
        stats = stream_measurement.get_streaming_statistics()
        
        if stats:
            print(f"\nStreaming Subscriber {subscriber_id} Statistics:")
            print(f"  Received: {stats['streaming_measurement_count']} messages")
            print(f"  Sync Offset: {stats['synchronization_offset']['mean_us']:.1f} ± {stats['synchronization_offset']['std_dev_us']:.1f} μs")
            print(f"  P95 Offset: {stats['synchronization_offset']['p95_us']:.1f} μs")
            print(f"  P99 Offset: {stats['synchronization_offset']['p99_us']:.1f} μs")
        
        subscriber.close()
        return stats


def parse_arguments():
    """Parse command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Comprehensive Synchronization Measurement')
    
    # Mode selection
    parser.add_argument('mode', choices=['ntp', 'streaming', 'full'], 
                       help='Measurement mode: ntp (REQ/REP), streaming (PUB/SUB), or full (both)')
    
    # Network configuration
    parser.add_argument('--server-ip', default='localhost', 
                       help='Server IP address (default: localhost)')
    parser.add_argument('--req-port', type=int, default=5555,
                       help='REQ/REP port (default: 5555)')
    parser.add_argument('--pub-port', type=int, default=5556,
                       help='PUB/SUB port (default: 5556)')
    
    # Data generation parameters
    parser.add_argument('--rate-hz', type=float, default=10.0,
                       help='Message generation rate in Hz (default: 10.0)')
    parser.add_argument('--duration', type=int, default=60,
                       help='Test duration in seconds (default: 60)')
    parser.add_argument('--message-sizes', nargs='+', type=int, 
                       default=[1024, 1048576, 10485760],
                       help='Message sizes in bytes (default: 1KB 1MB 10MB)')
    
    # Node identification
    parser.add_argument('--node-id', default=None,
                       help='Node identifier (auto-generated if not specified)')
    parser.add_argument('--role', choices=['client', 'server', 'publisher', 'subscriber'],
                       help='Node role for distributed testing')
    
    # Clock synchronization
    parser.add_argument('--no-clock-sync', action='store_true',
                       help='Disable clock synchronization (use relative timing)')
    
    # Output options
    parser.add_argument('--output-file', default=None,
                       help='Output file for measurements (default: auto-generated)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    
    return parser.parse_args()

def main():
    """Run comprehensive synchronization measurement test"""
    import sys
    import threading
    
    args = parse_arguments()
    
    # Create benchmark with specified ports
    benchmark = ComprehensiveBenchmark(req_port=args.req_port, pub_port=args.pub_port)
    
    # Set server IP for client connections
    benchmark.server_ip = args.server_ip
    
    if args.verbose:
        print(f"Configuration:")
        print(f"  Mode: {args.mode}")
        print(f"  Server: {args.server_ip}:{args.req_port}/{args.pub_port}")
        print(f"  Rate: {args.rate_hz} Hz")
        print(f"  Duration: {args.duration} seconds") 
        print(f"  Message sizes: {args.message_sizes} bytes")
        print(f"  Clock sync: {not args.no_clock_sync}")
    
    # Handle distributed testing mode
    if args.role:
        if args.role == 'server' and args.mode in ['ntp', 'full']:
            benchmark.run_ntp_server(duration_sec=args.duration + 5)
        elif args.role == 'client' and args.mode in ['ntp', 'full']:
            benchmark.run_ntp_client(rate_hz=args.rate_hz, duration_sec=args.duration, 
                                   message_sizes=args.message_sizes)
        elif args.role == 'publisher' and args.mode in ['streaming', 'full']:
            benchmark.run_streaming_publisher(rate_hz=args.rate_hz, duration_sec=args.duration,
                                            message_sizes=args.message_sizes)
        elif args.role == 'subscriber' and args.mode in ['streaming', 'full']:
            node_id = args.node_id or f"subscriber_{args.server_ip}_{args.pub_port}"
            benchmark.run_streaming_subscriber(subscriber_id=node_id, duration_sec=args.duration + 5)
        else:
            print(f"Invalid role '{args.role}' for mode '{args.mode}'")
        return
    
    # Automatic mode (single process testing)
    if args.mode == "ntp":
        print("=== NTP 4-Timestamp Measurement Test ===")
        
        # Start server thread
        server_thread = threading.Thread(target=benchmark.run_ntp_server, args=(args.duration + 5,))
        server_thread.daemon = True
        server_thread.start()
        
        time.sleep(1)  # Let server start
        
        # Run client
        benchmark.run_ntp_client(rate_hz=args.rate_hz, duration_sec=args.duration, 
                               message_sizes=args.message_sizes)
        server_thread.join()
        
    elif args.mode == "streaming":
        print("=== Streaming Synchronization Measurement Test ===")
        
        # Start publisher thread
        pub_thread = threading.Thread(target=benchmark.run_streaming_publisher, 
                                     args=(args.rate_hz, args.duration, args.message_sizes))
        pub_thread.daemon = True
        pub_thread.start()
        
        time.sleep(2)  # Let publisher start
        
        # Run subscriber
        node_id = args.node_id or "streaming_subscriber"
        benchmark.run_streaming_subscriber(subscriber_id=node_id, duration_sec=args.duration + 5)
        pub_thread.join()
        
    elif args.mode == "full":
        print("=== Full Comprehensive Synchronization Test ===")
        
        # Run NTP test
        print("\n1. Running NTP 4-Timestamp Test...")
        server_thread = threading.Thread(target=benchmark.run_ntp_server, args=(args.duration + 5,))
        server_thread.daemon = True
        server_thread.start()
        
        time.sleep(1)
        ntp_stats = benchmark.run_ntp_client(rate_hz=args.rate_hz, duration_sec=args.duration,
                                           message_sizes=args.message_sizes)
        server_thread.join()
        
        time.sleep(2)  # Brief pause
        
        # Run streaming test
        print("\n2. Running Streaming Synchronization Test...")
        pub_thread = threading.Thread(target=benchmark.run_streaming_publisher, 
                                     args=(args.rate_hz, args.duration, args.message_sizes))
        pub_thread.daemon = True
        pub_thread.start()
        
        time.sleep(2)
        node_id = args.node_id or "streaming_subscriber"
        streaming_stats = benchmark.run_streaming_subscriber(subscriber_id=node_id, 
                                                           duration_sec=args.duration + 5)
        pub_thread.join()
        
        print("\n=== COMPREHENSIVE TEST RESULTS ===")
        if ntp_stats:
            print(f"NTP Analysis:")
            print(f"  Clock Offset: {ntp_stats['clock_offset']['mean_us']:.1f} μs")
            print(f"  Network Delay: {ntp_stats['network_delay']['mean_us']:.1f} μs")
            print(f"  Processing Delay: {ntp_stats['processing_delay']['mean_us']:.1f} μs")
        
        if streaming_stats:
            print(f"Streaming Analysis:")
            print(f"  Synchronization Offset: {streaming_stats['synchronization_offset']['mean_us']:.1f} μs")
            print(f"  P95 Offset: {streaming_stats['synchronization_offset']['p95_us']:.1f} μs")


if __name__ == "__main__":
    main()