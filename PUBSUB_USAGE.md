# Producer-Consumer Synchronization Measurement

## Quick Start

```bash
# Terminal 1: Start consumer
python consumer.py -t 60

# Terminal 2: Start producer (10 Hz, 1KB messages, 60 seconds)
python producer.py -r 10 -t 60 -s 1024

# Results: producer_results.csv + network analysis
```

## Detailed Usage

### Producer Options
```bash
python producer.py -r 10 -t 60 -s 1024 \
    --data-port 5555 --viz-port 5556 --control-port 5557 \
    --viz-ip localhost --interface lo --output results.csv
```

### Consumer Options
```bash
python consumer.py -t 60 --processing-time-ms 0.4 \
    --viz-ip localhost --producer-data-port 5555 \
    --client-pub-port 5556 --control-port 5557
```

### Key Parameters
- `-r/--rate-hz`: Message rate (default: 10.0)
- `-t/--duration`: Test duration in seconds (default: 60)
- `-s/--message-size`: Message size in bytes (default: 1024)
- `--processing-time-ms`: Consumer processing delay (default: 0.4)
- `--interface`: Network interface for monitoring (default: lo0)
- `--output`: CSV output filename (default: producer_results.csv)