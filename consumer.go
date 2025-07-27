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

	// Main message processing loop
	startTime := time.Now()
	receivedCount := 0
	processingTimeDuration := time.Duration(processingTimeMs * float64(time.Millisecond))

	for time.Since(startTime).Seconds() < float64(durationSec) {
		// Receive message (non-blocking)
		parts, err := subSocket.RecvMessage(zmq.DONTWAIT)
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

		// Parse the raw message format
		parsed, err := parseRawMessage(parts[1])
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		// 4-timestamp NTP method timing
		t2 := timestampUs()
		time.Sleep(processingTimeDuration) // Configurable processing time
		t3 := timestampUs()

		// Create JSON response (same format as Python version)
		response := Response{
			Type:   "response",
			MsgID:  parsed.MsgID,
			SeqN:   parsed.SeqN,
			T2:     t2,
			T3:     t3,
			Status: "processed",
		}

		// Send JSON response back to producer
		responseBytes, err := json.Marshal(response)
		if err != nil {
			fmt.Printf("JSON marshal error: %v\n", err)
			continue
		}

		err = pubSocket.SendMessage("response", responseBytes)
		if err != nil {
			fmt.Printf("Send response error: %v\n", err)
			continue
		}

		receivedCount++

		// Progress reporting
		if receivedCount%100 == 0 {
			fmt.Printf("Consumer: processed %d\n", receivedCount)
		}
	}

	fmt.Printf("Consumer: processed %d messages in %.1f seconds\n", 
		receivedCount, time.Since(startTime).Seconds())

	return nil
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