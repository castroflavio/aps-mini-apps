# Pub-Sub Synchronization Measurement - Usage

## Overview

This implementation uses **only pub-sub patterns** with 4-timestamp NTP method at the application layer:

1. **Server → Client**: Server publishes messages (T1), client receives (T2)
2. **Client → Server**: Client publishes responses (T3), server receives (T4)
3. **NTP Analysis**: Clock offset, network delay, processing delay calculated from T1-T4

## Architecture

```
Server (Publisher/Subscriber)     Client (Subscriber/Publisher)
├── Publishes requests (T1) ────→ Receives requests (T2)
├── Receives responses (T4) ←──── Publishes responses (T3)
└── Calculates 4-timestamp NTP metrics
```

## Basic Usage

```bash
# Single process test (both server and client)
python pubsub_sync_measurement.py test

# Terminal 1: Start server
python pubsub_sync_measurement.py server

# Terminal 2: Start client
python pubsub_sync_measurement.py client
```

## F1.md Test Cases

### Single Process Tests
```bash
# F1.1: REQ/REP Offset Measurement (10 Hz, 60s, 1KB/1MB/10MB)
python pubsub_sync_measurement.py test --rate-hz 10 --duration 60 --message-sizes "1024,1048576,10485760"

# F1.2: Pipeline Chain Measurement (1 Hz, 120s, 1KB/1MB/10MB)
python pubsub_sync_measurement.py test --rate-hz 1 --duration 120 --message-sizes "1024,1048576,10485760"
```

### Distributed F1.1 Test
```bash
# Terminal 1: Server (controls rate and message sizes)
python pubsub_sync_measurement.py server --rate-hz 10 --duration 60 --message-sizes "1024,1048576,10485760"

# Terminal 2: Client (connects and responds)
python pubsub_sync_measurement.py client --duration 60
```

### Distributed F1.2 Test
```bash
# Terminal 1: Server
python pubsub_sync_measurement.py server --rate-hz 1 --duration 120 --message-sizes "1024,1048576,10485760"

# Terminal 2: Client
python pubsub_sync_measurement.py client --duration 120
```

### Remote Distributed Testing
```bash
# Terminal 1: Server on machine A
python pubsub_sync_measurement.py server --rate-hz 10 --duration 60 --message-sizes "1024,1048576,10485760"

# Terminal 2: Client on machine B
python pubsub_sync_measurement.py client --server-ip 192.168.1.100 --duration 60
```

## Arguments

- `--rate-hz`: Message rate (default: 10.0)
- `--duration`: Test duration in seconds (default: 60)
- `--message-sizes`: Sizes in bytes, comma-separated (default: 1KB,1MB,10MB)
- `--server-ip`: Server IP address (default: localhost)
- `--server-pub-port`: Server publisher port (default: 5555)
- `--client-pub-port`: Client publisher port (default: 5556)

## Output Example

```
=== Pub-Sub Sync Results (F1.md Format) ===
Rate: 10.0 Hz, Duration: 60s
Total measurements: 1800

Overall Statistics:
  Clock offset: 125.3 ± 45.2 μs
  Network delay: 845.1 ± 120.8 μs
  Processing delay: 400.2 ± 15.7 μs
  Total delay: 1370.6 ± 180.3 μs

Per-Message-Size Analysis:
  1KB messages (600 samples):
    Clock offset: 120.1 μs
    Network delay: 820.5 μs
    Processing delay: 400.1 μs
    Total delay: 1340.7 μs
  1MB messages (600 samples):
    Clock offset: 125.8 μs
    Network delay: 845.2 μs
    Processing delay: 400.2 μs
    Total delay: 1371.2 μs
  10MB messages (600 samples):
    Clock offset: 130.1 μs
    Network delay: 869.6 μs
    Processing delay: 400.3 μs
    Total delay: 1400.0 μs
```

## Output Files

- `server_pubsub_results.json`: Complete 4-timestamp measurements with NTP analysis

## JSON Output Format

```json
{
  "node_id": "server",
  "measurements": [
    {
      "seq_n": 1,
      "msg_id": "uuid-string",
      "timestamps": [1672531200000000, 1672531200001500, 1672531200002500, 1672531200004000],
      "clock_offset_us": 125,
      "network_delay_us": 850,
      "processing_delay_us": 400,
      "total_delay_us": 4000,
      "message_size": 1024
    }
  ],
  "count": 1800
}
```

## Benefits vs REQ/REP

1. **True streaming**: Continuous pub-sub flow matches real applications
2. **Bidirectional measurement**: Both directions measured simultaneously
3. **Scalability**: Multiple clients can subscribe to same server
4. **Realistic**: Matches actual distributed system communication patterns
5. **Integrated workflow**: Single unified measurement system

## Integration with Trace System

This pub-sub pattern directly matches the Trace pipeline architecture:

```
DAQ Service (Publisher) → Distributor (Subscriber/Publisher) → SIRT (Subscriber)
```

Each service can add timestamps to messages, enabling end-to-end pipeline measurement with full 4-timestamp NTP analysis.