package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// PubSubConsumer represents the consumer instance
type PubSubConsumer struct {
	nodeID             string
	producerDataPort   int
	vizPort            int
	producerIP         string
	controlPort        int
}

// Response structure for JSON response to producer
type Response struct {
	Type   string `json:"type"`
	MsgID  string `json:"msg_id"`
	SeqN   int    `json:"seq_n"`
	T2     int64  `json:"t2"`
	T3     int64  `json:"t3"`
	Status string `json:"status"`
}

// ParsedMessage holds the parsed raw message components
type ParsedMessage struct {
	SeqN  int
	MsgID string
	T1    int64
	Data  string
}

// NewPubSubConsumer creates a new consumer instance
func NewPubSubConsumer(nodeID string, producerDataPort, vizPort int, producerIP string, controlPort int) *PubSubConsumer {
	return &PubSubConsumer{
		nodeID:           nodeID,
		producerDataPort: producerDataPort,
		vizPort:          vizPort,
		producerIP:       producerIP,
		controlPort:      controlPort,
	}
}

// timestampUs returns current timestamp in microseconds
func timestampUs() int64 {
	return time.Now().UnixNano() / 1000
}

// parseRawMessage parses the raw format: request|seq_n=123|msg_id=456|t1=12345678901234567890|data=xxx
func parseRawMessage(msgStr string) (*ParsedMessage, error) {
	if !strings.HasPrefix(msgStr, "request|") {
		return nil, fmt.Errorf("invalid message format: missing 'request|' prefix")
	}

	// Split by pipe delimiter
	parts := strings.Split(msgStr, "|")
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid message format: insufficient parts (%d)", len(parts))
	}

	// Parse seq_n=123
	seqParts := strings.Split(parts[1], "=")
	if len(seqParts) != 2 || seqParts[0] != "seq_n" {
		return nil, fmt.Errorf("invalid seq_n format: %s", parts[1])
	}
	seqN, err := strconv.Atoi(seqParts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid seq_n value: %s", seqParts[1])
	}

	// Parse msg_id=456
	msgIdParts := strings.Split(parts[2], "=")
	if len(msgIdParts) != 2 || msgIdParts[0] != "msg_id" {
		return nil, fmt.Errorf("invalid msg_id format: %s", parts[2])
	}
	msgID := msgIdParts[1]

	// Parse t1=12345678901234567890
	t1Parts := strings.Split(parts[3], "=")
	if len(t1Parts) != 2 || t1Parts[0] != "t1" {
		return nil, fmt.Errorf("invalid t1 format: %s", parts[3])
	}
	t1, err := strconv.ParseInt(t1Parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid t1 value: %s", t1Parts[1])
	}

	// Extract data payload (everything after data=)
	dataStart := strings.Index(msgStr, "data=")
	var data string
	if dataStart != -1 {
		data = msgStr[dataStart+5:] // Skip "data="
	}

	return &ParsedMessage{
		SeqN:  seqN,
		MsgID: msgID,
		T1:    t1,
		Data:  data,
	}, nil
}

// runConsumer executes the main consumer logic
func (c *PubSubConsumer) runConsumer(durationSec int, processingTimeMs float64) error {
	// Create ZMQ context
	context, err := zmq.NewContext()
	if err != nil {
		return fmt.Errorf("failed to create ZMQ context: %v", err)
	}
	defer context.Term()

	// Subscriber socket (SUB) - receives from producer
	subSocket, err := context.NewSocket(zmq.SUB)
	if err != nil {
		return fmt.Errorf("failed to create SUB socket: %v", err)
	}
	defer subSocket.Close()

	err = subSocket.Connect(fmt.Sprintf("tcp://%s:%d", c.producerIP, c.producerDataPort))
	if err != nil {
		return fmt.Errorf("failed to connect to producer: %v", err)
	}

	err = subSocket.SetSubscribe("request")
	if err != nil {
		return fmt.Errorf("failed to set subscription: %v", err)
	}

	// Publisher socket (PUB) - sends responses back to producer
	pubSocket, err := context.NewSocket(zmq.PUB)
	if err != nil {
		return fmt.Errorf("failed to create PUB socket: %v", err)
	}
	defer pubSocket.Close()

	err = pubSocket.Bind(fmt.Sprintf("tcp://*:%d", c.vizPort))
	if err != nil {
		return fmt.Errorf("failed to bind publisher: %v", err)
	}

	fmt.Printf("Consumer: subscribing to %d, publishing on %d\n", c.producerDataPort, c.vizPort)
	time.Sleep(1 * time.Second) // Allow pub-sub connections to establish

	// Send start control message to producer
	controlSocket, err := context.NewSocket(zmq.REQ)
	if err != nil {
		return fmt.Errorf("failed to create control socket: %v", err)
	}
	defer controlSocket.Close()

	err = controlSocket.Connect(fmt.Sprintf("tcp://%s:%d", c.producerIP, c.controlPort))
	if err != nil {
		return fmt.Errorf("failed to connect to control port: %v", err)
	}

	_, err = controlSocket.Send("start", 0)
	if err != nil {
		return fmt.Errorf("failed to send start signal: %v", err)
	}

	_, err = controlSocket.Recv(0)
	if err != nil {
		return fmt.Errorf("failed to receive start acknowledgment: %v", err)
	}

	fmt.Println("Consumer: start signal sent, waiting for messages...")

	// Main message processing loop with instrumentation
	startTime := time.Now()
	receivedCount := 0
	processingTimeDuration := time.Duration(processingTimeMs * float64(time.Millisecond))
	
	// Timing instrumentation arrays
	recvTimes := make([]float64, 0, 10000)      // ZMQ receive times
	parseTimes := make([]float64, 0, 10000)     // Message parsing times
	processTimes := make([]float64, 0, 10000)   // Processing delay times
	sendTimes := make([]float64, 0, 10000)      // Response send times
	totalBytes := int64(0)                      // Total data processed
	lastReportTime := startTime
	lastReportCount := 0

	for time.Since(startTime).Seconds() < float64(durationSec) {
		// Receive message with timing
		recvStart := time.Now()
		parts, err := subSocket.RecvMessage(zmq.DONTWAIT)
		recvTime := time.Since(recvStart).Seconds() * 1e6 // Convert to microseconds
		
		if err != nil {
			if err.Error() == "resource temporarily unavailable" {
				time.Sleep(1 * time.Millisecond)
				continue
			}
			fmt.Printf("Consumer error: %v\n", err)
			break
		}

		if len(parts) != 2 || parts[0] != "request" {
			continue // Skip malformed messages
		}

		// Parse message with timing
		parseStart := time.Now()
		parsed, err := parseRawMessage(parts[1])
		parseTime := time.Since(parseStart).Seconds() * 1e6
		
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		// 4-timestamp NTP method timing
		t2 := timestampUs()
		processStart := time.Now()
		time.Sleep(processingTimeDuration) // Configurable processing time
		processTime := time.Since(processStart).Seconds() * 1e6
		t3 := timestampUs()

		// Create and send response with timing
		response := Response{
			Type:   "response",
			MsgID:  parsed.MsgID,
			SeqN:   parsed.SeqN,
			T2:     t2,
			T3:     t3,
			Status: "processed",
		}

		responseBytes, err := json.Marshal(response)
		if err != nil {
			fmt.Printf("JSON marshal error: %v\n", err)
			continue
		}

		sendStart := time.Now()
		_, err = pubSocket.SendMessage("response", responseBytes)
		sendTime := time.Since(sendStart).Seconds() * 1e6
		
		if err != nil {
			fmt.Printf("Send response error: %v\n", err)
			continue
		}

		// Record timing data (sample every 10th message to avoid memory issues)
		if receivedCount%10 == 0 && len(recvTimes) < 10000 {
			recvTimes = append(recvTimes, recvTime)
			parseTimes = append(parseTimes, parseTime)
			processTimes = append(processTimes, processTime)
			sendTimes = append(sendTimes, sendTime)
		}
		
		totalBytes += int64(len(parts[1]))
		receivedCount++

		// Real-time rate reporting every 5 seconds
		if time.Since(lastReportTime).Seconds() >= 5.0 {
			elapsed := time.Since(lastReportTime).Seconds()
			recentCount := receivedCount - lastReportCount
			currentRate := float64(recentCount) / elapsed
			recentBytes := float64(recentCount) * float64(len(parts[1]))
			throughputMbps := (recentBytes * 8) / (elapsed * 1e6)
			
			fmt.Printf("Consumer: %d msg (%.1f Hz) | %.1f Mbps | Total: %d\n", 
				recentCount, currentRate, throughputMbps, receivedCount)
			
			lastReportTime = time.Now()
			lastReportCount = receivedCount
		}
	}

	elapsedTime := time.Since(startTime).Seconds()
	avgRate := float64(receivedCount) / elapsedTime
	avgThroughputMbps := (float64(totalBytes) * 8) / (elapsedTime * 1e6)

	fmt.Printf("\n=== Consumer Performance Summary ===\n")
	fmt.Printf("Processed: %d messages in %.1f seconds\n", receivedCount, elapsedTime)
	fmt.Printf("Average rate: %.1f Hz | Throughput: %.1f Mbps (%.2f Gbps)\n", 
		avgRate, avgThroughputMbps, avgThroughputMbps/1000)

	// Detailed timing analysis
	if len(recvTimes) > 0 {
		fmt.Printf("\n=== Bottleneck Analysis ===\n")
		fmt.Printf("Operation breakdown (µs - mean/P95/P99):\n")
		
		recvMean, recvP95, recvP99 := calculateStats(recvTimes)
		parseMean, parseP95, parseP99 := calculateStats(parseTimes)
		processMean, processP95, processP99 := calculateStats(processTimes)
		sendMean, sendP95, sendP99 := calculateStats(sendTimes)
		
		fmt.Printf("  ZMQ Receive: %.0f/%.0f/%.0f µs\n", recvMean, recvP95, recvP99)
		fmt.Printf("  Message Parse: %.0f/%.0f/%.0f µs\n", parseMean, parseP95, parseP99)
		fmt.Printf("  Processing: %.0f/%.0f/%.0f µs (configured: %.0f µs)\n", 
			processMean, processP95, processP99, processingTimeMs*1000)
		fmt.Printf("  Response Send: %.0f/%.0f/%.0f µs\n", sendMean, sendP95, sendP99)
		
		totalMean := recvMean + parseMean + processMean + sendMean
		maxTheoretical := 1e6 / totalMean
		
		fmt.Printf("  Total per message: %.0f µs\n", totalMean)
		fmt.Printf("  Theoretical max rate: %.0f Hz\n", maxTheoretical)
		
		// Identify bottleneck
		bottleneck := "Processing (configured delay)"
		bottleneckTime := processMean
		if recvMean > bottleneckTime {
			bottleneck = "ZMQ Receive"
			bottleneckTime = recvMean
		}
		if parseMean > bottleneckTime {
			bottleneck = "Message Parsing"
			bottleneckTime = parseMean
		}
		if sendMean > bottleneckTime {
			bottleneck = "Response Send"
			bottleneckTime = sendMean
		}
		
		fmt.Printf("  Primary bottleneck: %s (%.0f µs = %.1f%% of total)\n", 
			bottleneck, bottleneckTime, (bottleneckTime/totalMean)*100)
		
		if avgRate < maxTheoretical * 0.8 {
			fmt.Printf("⚠️  Consumer is likely the bottleneck (%.1f%% of theoretical max)\n", 
				(avgRate/maxTheoretical)*100)
		} else {
			fmt.Printf("✅ Consumer keeping up with producer\n")
		}
	}

	return nil
}

// calculateStats computes mean, P95, and P99 from timing data
func calculateStats(values []float64) (mean, p95, p99 float64) {
	if len(values) == 0 {
		return 0, 0, 0
	}

	// Calculate mean
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean = sum / float64(len(values))

	// Sort for percentiles (simple bubble sort for small datasets)
	sorted := make([]float64, len(values))
	copy(sorted, values)
	
	for i := 0; i < len(sorted); i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			if sorted[j] > sorted[j+1] {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}

	// Calculate percentiles
	p95Index := int(0.95 * float64(len(sorted)-1))
	p99Index := int(0.99 * float64(len(sorted)-1))
	
	p95 = sorted[p95Index]
	p99 = sorted[p99Index]

	return mean, p95, p99
}

func main() {
	// Command line flags (matching Python consumer interface)
	duration := flag.Int("t", 60, "Duration seconds")
	producerIP := flag.String("producer-ip", "localhost", "Producer IP address")
	producerDataPort := flag.Int("producer-data-port", 5555, "Producer publisher port")
	vizPort := flag.Int("viz-port", 5556, "Consumer publisher port")
	controlPort := flag.Int("control-port", 5557, "Control channel port")
	processingTimeMs := flag.Float64("processing-time-ms", 0.4, "Processing time in milliseconds")
	flag.Parse()

	fmt.Printf("Go Pub-Sub Consumer v1.0 - High Performance\n")
	fmt.Printf("Duration: %ds | Processing time: %.1fms\n", *duration, *processingTimeMs)
	fmt.Printf("Connecting to producer at %s:%d\n", *producerIP, *producerDataPort)

	consumer := NewPubSubConsumer("consumer", *producerDataPort, *vizPort, *producerIP, *controlPort)
	
	err := consumer.runConsumer(*duration+10, *processingTimeMs) // +10 seconds buffer like Python
	if err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
}