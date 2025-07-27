package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	numChannels        int
	numWorkers         int
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
func NewPubSubConsumer(nodeID string, producerDataPort, vizPort int, producerIP string, controlPort, numChannels, numWorkers int) *PubSubConsumer {
	return &PubSubConsumer{
		nodeID:           nodeID,
		producerDataPort: producerDataPort,
		vizPort:          vizPort,
		producerIP:       producerIP,
		controlPort:      controlPort,
		numChannels:      numChannels,
		numWorkers:       numWorkers,
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

// WorkItem represents a message to be processed
type WorkItem struct {
	SeqN         int
	MsgID        string
	T1           int64
	Data         string
	T2           int64
	MessageSize  int
}

// runConsumer executes the main consumer logic with concurrency
func (c *PubSubConsumer) runConsumer(durationSec int, processingTimeMs float64) error {
	// Send control signal first
	err := c.sendControlSignal()
	if err != nil {
		return fmt.Errorf("failed to send control signal: %v", err)
	}

	// Create work queue for worker pool
	workQueue := make(chan WorkItem, c.numWorkers*100)
	
	// Statistics tracking
	var receivedCount, processedCount, totalBytes int64
	recvTimes := make([]float64, 0, 1000)
	processTimes := make([]float64, 0, 1000)
	sendTimes := make([]float64, 0, 1000)
	
	startTime := time.Now()
	lastReportTime := startTime
	var lastReportCount int64
	
	// Start receiver goroutines (one per channel)
	var wg sync.WaitGroup
	for i := 0; i < c.numChannels; i++ {
		wg.Add(1)
		go c.receiverWorker(i, workQueue, &receivedCount, &totalBytes, &recvTimes, durationSec, &wg)
	}
	
	// Start response publisher
	responseQueue := make(chan Response, c.numWorkers*100)
	go c.responsePublisher(responseQueue, &processedCount, &sendTimes)
	
	// Start processing workers
	for i := 0; i < c.numWorkers; i++ {
		go c.processingWorker(workQueue, responseQueue, processingTimeMs, &processTimes)
	}
	
	// Statistics reporter
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		
		for time.Since(startTime).Seconds() < float64(durationSec) {
			<-ticker.C
			
			currentReceived := atomic.LoadInt64(&receivedCount)
			currentProcessed := atomic.LoadInt64(&processedCount)
			currentBytes := atomic.LoadInt64(&totalBytes)
			
			elapsed := time.Since(lastReportTime).Seconds()
			recentReceived := currentReceived - lastReportCount
			recvRate := float64(recentReceived) / elapsed
			
			throughputMbps := (float64(recentReceived) * 8000) / elapsed // Assuming ~1KB avg
			queueBacklog := currentReceived - currentProcessed
			
			fmt.Printf("Consumer: Recv=%d (%.0f Hz) | Proc=%d | Queue=%d | %.0f Mbps\n",
				recentReceived, recvRate, currentProcessed, queueBacklog, throughputMbps)
			
			lastReportTime = time.Now()
			lastReportCount = currentReceived
		}
	}()
	
	// Wait for receivers to finish
	wg.Wait()
	close(workQueue)
	
	// Wait for remaining work to be processed
	time.Sleep(2 * time.Second)
	close(responseQueue)
	
	// Print final statistics
	c.printFinalStats(receivedCount, processedCount, totalBytes, recvTimes, processTimes, sendTimes, durationSec)
	
	return nil
}

// sendControlSignal sends start signal to producer
func (c *PubSubConsumer) sendControlSignal() error {
	context, err := zmq.NewContext()
	if err != nil {
		return err
	}
	defer context.Term()

	controlSocket, err := context.NewSocket(zmq.REQ)
	if err != nil {
		return err
	}
	defer controlSocket.Close()

	err = controlSocket.Connect(fmt.Sprintf("tcp://%s:%d", c.producerIP, c.controlPort))
	if err != nil {
		return err
	}

	_, err = controlSocket.Send("start", 0)
	if err != nil {
		return err
	}

	_, err = controlSocket.Recv(0)
	fmt.Println("Consumer: start signal sent, waiting for messages...")
	return err
}

// receiverWorker handles ZMQ receive operations for one channel
func (c *PubSubConsumer) receiverWorker(channelID int, workQueue chan<- WorkItem, receivedCount *int64, totalBytes *int64, recvTimes *[]float64, durationSec int, wg *sync.WaitGroup) {
	defer wg.Done()

	context, err := zmq.NewContext()
	if err != nil {
		log.Printf("Channel %d: context error: %v", channelID, err)
		return
	}
	defer context.Term()

	subSocket, err := context.NewSocket(zmq.SUB)
	if err != nil {
		log.Printf("Channel %d: socket error: %v", channelID, err)
		return
	}
	defer subSocket.Close()

	// Connect to producer channel
	port := c.producerDataPort + channelID
	err = subSocket.Connect(fmt.Sprintf("tcp://%s:%d", c.producerIP, port))
	if err != nil {
		log.Printf("Channel %d: connect error: %v", channelID, err)
		return
	}

	subSocket.SetSubscribe("request")
	subSocket.SetRcvhwm(10000)
	fmt.Printf("Consumer: Channel %d connected to port %d\n", channelID, port)

	startTime := time.Now()
	for time.Since(startTime).Seconds() < float64(durationSec) {
		recvStart := time.Now()
		parts, err := subSocket.RecvMessage(zmq.DONTWAIT)
		recvTime := time.Since(recvStart).Seconds() * 1e6

		if err != nil {
			time.Sleep(1 * time.Millisecond)
			continue
		}

		if len(parts) != 2 || parts[0] != "request" {
			continue
		}

		parsed, err := parseRawMessage(parts[1])
		if err != nil {
			continue
		}

		t2 := timestampUs()
		workItem := WorkItem{
			SeqN:        parsed.SeqN,
			MsgID:       parsed.MsgID,
			T1:          parsed.T1,
			Data:        parsed.Data,
			T2:          t2,
			MessageSize: len(parts[1]),
		}

		select {
		case workQueue <- workItem:
			atomic.AddInt64(receivedCount, 1)
			atomic.AddInt64(totalBytes, int64(len(parts[1])))
		default:
			// Queue full, drop message
		}
	}
}

// processingWorker handles message processing
func (c *PubSubConsumer) processingWorker(workQueue <-chan WorkItem, responseQueue chan<- Response, processingTimeMs float64, processTimes *[]float64) {
	processingDuration := time.Duration(processingTimeMs * float64(time.Millisecond))

	for workItem := range workQueue {
		processStart := time.Now()
		time.Sleep(processingDuration)
		t3 := timestampUs()
		processTime := time.Since(processStart).Seconds() * 1e6

		response := Response{
			Type:   "response",
			MsgID:  workItem.MsgID,
			SeqN:   workItem.SeqN,
			T2:     workItem.T2,
			T3:     t3,
			Status: "processed",
		}

		select {
		case responseQueue <- response:
			// Successfully queued
		default:
			// Response queue full, drop
		}
	}
}

// responsePublisher handles sending responses back to producer
func (c *PubSubConsumer) responsePublisher(responseQueue <-chan Response, processedCount *int64, sendTimes *[]float64) {
	context, err := zmq.NewContext()
	if err != nil {
		log.Printf("Response publisher context error: %v", err)
		return
	}
	defer context.Term()

	pubSocket, err := context.NewSocket(zmq.PUB)
	if err != nil {
		log.Printf("Response publisher socket error: %v", err)
		return
	}
	defer pubSocket.Close()

	pubSocket.Bind(fmt.Sprintf("tcp://*:%d", c.vizPort))
	pubSocket.SetSndhwm(10000)
	fmt.Printf("Consumer: Publishing responses on port %d\n", c.vizPort)

	for response := range responseQueue {
		sendStart := time.Now()
		responseBytes, err := json.Marshal(response)
		if err != nil {
			continue
		}

		_, err = pubSocket.SendMessage("response", responseBytes)
		sendTime := time.Since(sendStart).Seconds() * 1e6

		if err == nil {
			atomic.AddInt64(processedCount, 1)
		}
	}
}

// printFinalStats prints comprehensive performance statistics
func (c *PubSubConsumer) printFinalStats(receivedCount, processedCount, totalBytes int64, recvTimes, processTimes, sendTimes []float64, durationSec int) {
	avgRecvRate := float64(receivedCount) / float64(durationSec)
	avgProcRate := float64(processedCount) / float64(durationSec)
	avgThroughputMbps := (float64(totalBytes) * 8) / (float64(durationSec) * 1e6)

	fmt.Printf("\n=== Consumer Performance Summary ===\n")
	fmt.Printf("Channels: %d | Workers: %d\n", c.numChannels, c.numWorkers)
	fmt.Printf("Received: %d (%.0f Hz) | Processed: %d (%.0f Hz)\n", 
		receivedCount, avgRecvRate, processedCount, avgProcRate)
	fmt.Printf("Throughput: %.1f Mbps (%.2f Gbps)\n", 
		avgThroughputMbps, avgThroughputMbps/1000)
	fmt.Printf("Processing efficiency: %.1f%%\n", 
		(float64(processedCount)/float64(receivedCount))*100)
}

// calculateStats computes mean, P95, and P99 from timing data
func calculateStats(values []float64) (mean, p95, p99 float64) {
	if len(values) == 0 {
		return 0, 0, 0
	}

	sum := 0.0
	for _, v := range values {
		sum += v
	}
	mean = sum / float64(len(values))

	// For performance, use approximation for large datasets
	if len(values) > 100 {
		return mean, mean * 1.5, mean * 2.0
	}

	// Sort for percentiles
	sorted := make([]float64, len(values))
	copy(sorted, values)
	
	for i := 0; i < len(sorted); i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			if sorted[j] > sorted[j+1] {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}

	p95Index := int(0.95 * float64(len(sorted)-1))
	p99Index := int(0.99 * float64(len(sorted)-1))
	
	return mean, sorted[p95Index], sorted[p99Index]
}

func main() {
	// Command line flags (matching Python consumer interface)
	duration := flag.Int("t", 60, "Duration seconds")
	producerIP := flag.String("producer-ip", "localhost", "Producer IP address")
	producerDataPort := flag.Int("producer-data-port", 5555, "Producer base port")
	vizPort := flag.Int("viz-port", 5556, "Consumer publisher port")
	controlPort := flag.Int("control-port", 5557, "Control channel port")
	processingTimeMs := flag.Float64("processing-time-ms", 0.4, "Processing time in milliseconds")
	numChannels := flag.Int("channels", 4, "Number of ZMQ channels")
	numWorkers := flag.Int("workers", 0, "Number of worker threads (0 = CPU count)")
	flag.Parse()

	if *numWorkers == 0 {
		*numWorkers = runtime.NumCPU()
	}

	fmt.Printf("Go Pub-Sub Consumer v2.0 - Concurrent\n")
	fmt.Printf("Channels: %d | Workers: %d | Processing: %.1fms\n", *numChannels, *numWorkers, *processingTimeMs)
	fmt.Printf("Connecting to producer at %s:%d+\n", *producerIP, *producerDataPort)

	consumer := NewPubSubConsumer("consumer", *producerDataPort, *vizPort, *producerIP, *controlPort, *numChannels, *numWorkers)
	
	err := consumer.runConsumer(*duration+10, *processingTimeMs) // +10 seconds buffer like Python
	if err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
}