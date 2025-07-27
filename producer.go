package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// Measurement represents a 4-timestamp NTP measurement
type Measurement struct {
	SeqN            int     `json:"seq_n"`
	MsgID           string  `json:"msg_id"`
	T1, T2, T3, T4  int64   `json:"t1,t2,t3,t4"`
	MessageSize     int     `json:"message_size"`
	ClockOffsetMs   int64   `json:"clock_offset_ms"`
	NetworkDelayMs  int64   `json:"network_delay_ms"`
	ProcessingDelayMs int64 `json:"processing_delay_ms"`
	TotalDelayMs    int64   `json:"total_delay_ms"`
}

// PubSubProducer represents the producer instance
type PubSubProducer struct {
	nodeID          string
	dataPort        int
	vizPort         int
	vizIP           string
	controlPort     int
	measurements    []Measurement
	pendingMessages map[string]PendingMessage
	mu              sync.RWMutex
	streamingStarted bool
}

// PendingMessage holds message metadata
type PendingMessage struct {
	SeqN    int   `json:"seq_n"`
	T1      int64 `json:"t1"`
	MsgSize int   `json:"msg_size"`
}

// Response from consumer
type Response struct {
	Type   string `json:"type"`
	MsgID  string `json:"msg_id"`
	SeqN   int    `json:"seq_n"`
	T2     int64  `json:"t2"`
	T3     int64  `json:"t3"`
	Status string `json:"status"`
}

// NewPubSubProducer creates a new producer instance
func NewPubSubProducer(nodeID string, dataPort, vizPort int, vizIP string, controlPort int) *PubSubProducer {
	return &PubSubProducer{
		nodeID:          nodeID,
		dataPort:        dataPort,
		vizPort:         vizPort,
		vizIP:           vizIP,
		controlPort:     controlPort,
		measurements:    make([]Measurement, 0),
		pendingMessages: make(map[string]PendingMessage),
	}
}

// timestampUs returns current timestamp in microseconds
func timestampUs() int64 {
	return time.Now().UnixNano() / 1000
}

// createMeasurement creates a measurement from timestamps
func createMeasurement(seqN int, msgID string, t1, t2, t3, t4 int64, msgSize int) Measurement {
	return Measurement{
		SeqN:              seqN,
		MsgID:             msgID,
		T1:                t1,
		T2:                t2,
		T3:                t3,
		T4:                t4,
		MessageSize:       msgSize,
		ClockOffsetMs:     ((t2 - t1) + (t3 - t4)) / 2000,
		NetworkDelayMs:    ((t4 - t1) - (t3 - t2)) / 2000,
		ProcessingDelayMs: (t3 - t2) / 1000,
		TotalDelayMs:      (t4 - t1) / 1000,
	}
}

// prepareMessages creates pre-allocated message templates with fixed-width timestamp slots
func (p *PubSubProducer) prepareMessages(totalMessages, msgSize int) [][]byte {
	dataPayload := strings.Repeat("x", msgSize)
	messages := make([][]byte, totalMessages)
	
	for i := 0; i < totalMessages; i++ {
		// Format: request|seq_n=123|msg_id=456|t1=12345678901234567890|data=xxx
		template := fmt.Sprintf("request|seq_n=%d|msg_id=%d|t1=%020d|data=%s", 
			i, i, 0, dataPayload)
		messages[i] = []byte(template)
	}
	
	return messages
}

// setupNetworking initializes ZMQ sockets and network monitoring
func (p *PubSubProducer) setupNetworking(iface string, durationSec int) (*zmq.Socket, *zmq.Socket, *zmq.Socket, *exec.Cmd, string, error) {
	// Control socket (REP)
	controlSocket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	err = controlSocket.Bind(fmt.Sprintf("tcp://*:%d", p.controlPort))
	if err != nil {
		return nil, nil, nil, nil, "", err
	}

	// Publisher socket (PUB)
	pubSocket, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	err = pubSocket.SetSndhwm(1000)
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	err = pubSocket.Bind(fmt.Sprintf("tcp://*:%d", p.dataPort))
	if err != nil {
		return nil, nil, nil, nil, "", err
	}

	// Visualization socket (SUB)
	vizSocket, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	err = vizSocket.Connect(fmt.Sprintf("tcp://%s:%d", p.vizIP, p.vizPort))
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	err = vizSocket.SetSubscribe("response")
	if err != nil {
		return nil, nil, nil, nil, "", err
	}

	// Start visualization listener
	go p.visualizationListener(vizSocket)

	// Start network monitoring
	networkCSV := fmt.Sprintf("network_%s_%d.csv", iface, time.Now().Unix())
	monitorProc := exec.Command("python", "network_throughput_monitor.py", 
		"-i", iface, "-d", strconv.Itoa(durationSec+10), "-o", networkCSV)
	monitorProc.Start()

	time.Sleep(200 * time.Millisecond)
	return controlSocket, pubSocket, vizSocket, monitorProc, networkCSV, nil
}

// waitForConsumer waits for consumer ready signal
func (p *PubSubProducer) waitForConsumer(controlSocket *zmq.Socket) error {
	_, err := controlSocket.Recv(0)
	if err != nil {
		return err
	}
	_, err = controlSocket.Send("start", 0)
	if err != nil {
		return err
	}
	controlSocket.Close()
	return nil
}

// updateTimestamp efficiently updates timestamp in pre-allocated message
func updateTimestamp(message []byte, timestamp int64) {
	timestampStr := fmt.Sprintf("%020d", timestamp)
	timestampBytes := []byte(timestampStr)
	
	// Find t1= position and update the 20 bytes after it
	for i := 0; i < len(message)-23; i++ {
		if string(message[i:i+3]) == "t1=" {
			copy(message[i+3:i+23], timestampBytes)
			break
		}
	}
}

// streamMessages sends messages at specified rate with timing analysis
func (p *PubSubProducer) streamMessages(pubSocket *zmq.Socket, preparedMessages [][]byte, 
	rateHz float64, durationSec int, msgSize int) map[string]interface{} {
	
	startTime := time.Now()
	interval := time.Duration(float64(time.Second) / rateHz)
	nextSendTime := time.Now()
	seqN := 0
	sentCount := 0
	skipped := 0
	
	// Timing arrays for statistical analysis
	prepTimes := make([]float64, 0, len(preparedMessages))
	
	for seqN < len(preparedMessages) && time.Since(startTime).Seconds() < float64(durationSec) {
		msgID := strconv.Itoa(seqN)
		t1 := timestampUs()
		
		// Message preparation timing (equivalent to string concatenation in Python)
		prepStart := time.Now()
		
		// Create a copy of the message template and update timestamp
		message := make([]byte, len(preparedMessages[seqN]))
		copy(message, preparedMessages[seqN])
		updateTimestamp(message, t1)
		
		prepTime := time.Since(prepStart).Seconds() * 1e6 // Convert to microseconds
		prepTimes = append(prepTimes, prepTime)
		
		// Store pending message for response tracking
		p.mu.Lock()
		p.pendingMessages[msgID] = PendingMessage{
			SeqN:    seqN,
			T1:      t1,
			MsgSize: msgSize,
		}
		p.mu.Unlock()
		
		// Send message (non-blocking)
		err := pubSocket.SendMessage("request", message)
		if err != nil {
			skipped++
			p.mu.Lock()
			delete(p.pendingMessages, msgID)
			p.mu.Unlock()
			if seqN%100 == 0 {
				fmt.Printf("ZMQ queue full! Skipped %d/%d messages (%.1f%%)\n", 
					skipped, seqN, float64(skipped)/float64(seqN)*100)
			}
		} else {
			sentCount++
		}
		
		seqN++
		nextSendTime = nextSendTime.Add(interval)
		sleepTime := time.Until(nextSendTime)
		
		if sleepTime > 0 {
			time.Sleep(sleepTime)
		} else if seqN%1000 == 0 {
			behindMs := -sleepTime.Seconds() * 1000
			currentRate := float64(sentCount) / time.Since(startTime).Seconds()
			fmt.Printf("WARNING: %.1fms behind, rate: %.1fHz (target: %.1fHz)\n", 
				behindMs, currentRate, rateHz)
		}
	}
	
	return map[string]interface{}{
		"seq_n":       seqN,
		"sent_count":  sentCount,
		"skipped":     skipped,
		"elapsed_time": time.Since(startTime).Seconds(),
		"prep_times":   prepTimes,
	}
}

// analyzePerformance prints detailed performance statistics
func (p *PubSubProducer) analyzePerformance(results map[string]interface{}, rateHz float64, msgSize int) {
	seqN := results["seq_n"].(int)
	sentCount := results["sent_count"].(int) 
	skipped := results["skipped"].(int)
	elapsedTime := results["elapsed_time"].(float64)
	prepTimes := results["prep_times"].([]float64)
	
	throughputGbps := float64(sentCount*msgSize*8) / (elapsedTime * 1e9)
	
	fmt.Printf("\n=== Producer Summary ===\n")
	fmt.Printf("Messages: %d/%d sent (%d skipped = %.1f%%)\n", 
		sentCount, seqN, skipped, float64(skipped)/float64(seqN)*100)
	fmt.Printf("Rate: %.1f Hz (target: %.1f Hz) | Throughput: %.2f Gbps\n", 
		float64(sentCount)/elapsedTime, rateHz, throughputGbps)
	
	// Statistical analysis of preparation times
	if len(prepTimes) > 0 {
		meanUs := mean(prepTimes)
		p70Us := percentile(prepTimes, 70)
		p99Us := percentile(prepTimes, 99)
		targetIntervalUs := 1e6 / rateHz
		prepOverheadPct := (meanUs / targetIntervalUs) * 100
		
		fmt.Printf("\n=== Message Prep Analysis ===\n")
		fmt.Printf("Prep time: Mean=%.0fµs, P70=%.0fµs, P99=%.0fµs [in-place]\n", 
			meanUs, p70Us, p99Us)
		fmt.Printf("Target interval: %.0fµs | Overhead: %.1f%% | Max rate: %.0f Hz\n", 
			targetIntervalUs, prepOverheadPct, 1e6/meanUs)
		
		if prepOverheadPct > 50 {
			fmt.Printf("⚠️  Rate too high for message size (%.0f%% overhead)\n", prepOverheadPct)
		} else if prepOverheadPct > 20 {
			fmt.Printf("⚠️  Approaching limits (%.0f%% overhead)\n", prepOverheadPct)
		} else {
			fmt.Printf("✅ Acceptable overhead (%.0f%%)\n", prepOverheadPct)
		}
	}
}

// visualizationListener handles incoming responses from consumer
func (p *PubSubProducer) visualizationListener(vizSocket *zmq.Socket) {
	for {
		parts, err := vizSocket.RecvMessage(zmq.DONTWAIT)
		if err != nil {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		
		if len(parts) != 2 || parts[0] != "response" {
			continue
		}
		
		var response Response
		err = json.Unmarshal([]byte(parts[1]), &response)
		if err != nil {
			continue
		}
		
		if response.Type == "response" {
			p.mu.Lock()
			if pending, exists := p.pendingMessages[response.MsgID]; exists {
				delete(p.pendingMessages, response.MsgID)
				t4 := timestampUs()
				
				measurement := createMeasurement(
					pending.SeqN, response.MsgID, pending.T1, 
					response.T2, response.T3, t4, pending.MsgSize)
				p.measurements = append(p.measurements, measurement)
			}
			p.mu.Unlock()
		}
	}
}

// runProducer executes the main producer logic
func (p *PubSubProducer) runProducer(rateHz float64, durationSec, msgSize int, iface string) (string, error) {
	controlSocket, pubSocket, vizSocket, monitorProc, networkCSV, err := p.setupNetworking(iface, durationSec)
	if err != nil {
		return "", err
	}
	defer pubSocket.Close()
	defer vizSocket.Close()

	totalMessages := int(rateHz * float64(durationSec))
	preparedMessages := p.prepareMessages(totalMessages, msgSize)
	
	err = p.waitForConsumer(controlSocket)
	if err != nil {
		return "", err
	}
	
	p.streamingStarted = true
	results := p.streamMessages(pubSocket, preparedMessages, rateHz, durationSec, msgSize)
	p.analyzePerformance(results, rateHz, msgSize)
	
	time.Sleep(5 * time.Second)
	monitorProc.Wait()
	
	return networkCSV, nil
}

// Utility functions for statistics
func mean(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func percentile(values []float64, p int) float64 {
	if len(values) == 0 {
		return 0
	}
	
	// Simple percentile calculation (for production, use a proper sort)
	sorted := make([]float64, len(values))
	copy(sorted, values)
	
	// Basic bubble sort for simplicity
	for i := 0; i < len(sorted); i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			if sorted[j] > sorted[j+1] {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}
	
	index := float64(p) / 100.0 * float64(len(sorted)-1)
	return sorted[int(index)]
}

func main() {
	// Command line flags
	rate := flag.Float64("r", 10.0, "Message rate Hz")
	duration := flag.Int("t", 60, "Duration seconds")
	msgSize := flag.Int("s", 1024, "Message size in bytes")
	dataPort := flag.Int("data-port", 5555, "Producer publisher port")
	vizPort := flag.Int("viz-port", 5556, "Client publisher port")
	controlPort := flag.Int("control-port", 5557, "Control channel port")
	vizIP := flag.String("viz-ip", "localhost", "Visualization IP address")
	iface := flag.String("i", "lo0", "Network interface for monitoring")
	output := flag.String("output", "producer_results.csv", "Output CSV filename")
	flag.Parse()

	fmt.Printf("Go Pub-Sub Producer v1.0 - High Performance\n")
	fmt.Printf("Rate: %.1f Hz | Duration: %ds | Message Size: %d bytes\n", *rate, *duration, *msgSize)

	producer := NewPubSubProducer("producer", *dataPort, *vizPort, *vizIP, *controlPort)
	networkCSV, err := producer.runProducer(*rate, *duration, *msgSize, *iface)
	if err != nil {
		log.Fatalf("Producer error: %v", err)
	}

	// Print measurement results
	if len(producer.measurements) > 0 {
		var offsets, delays, processing, totalDelays []float64
		
		for _, m := range producer.measurements {
			offsets = append(offsets, float64(m.ClockOffsetMs))
			delays = append(delays, float64(m.NetworkDelayMs))
			processing = append(processing, float64(m.ProcessingDelayMs))
			totalDelays = append(totalDelays, float64(m.TotalDelayMs))
		}

		fmt.Printf("\n=== Producer Results ===\n")
		fmt.Printf("Completed: %d measurements\n", len(producer.measurements))
		fmt.Printf("Offset: %.3fms (P99: %.3fms)\n", mean(offsets), percentile(offsets, 99))
		fmt.Printf("Delay: %.3fms (P99: %.3fms)\n", mean(delays), percentile(delays, 99))
		fmt.Printf("Processing: %.3fms (P99: %.3fms)\n", mean(processing), percentile(processing, 99))
		fmt.Printf("Total: %.3fms (P99: %.3fms)\n", mean(totalDelays), percentile(totalDelays, 99))

		// Export to CSV (simplified - in production use proper CSV library)
		file, err := os.Create(*output)
		if err == nil {
			defer file.Close()
			fmt.Fprintf(file, "seq_n,msg_id,t1,t2,t3,t4,message_size,clock_offset_ms,network_delay_ms,processing_delay_ms,total_delay_ms\n")
			for _, m := range producer.measurements {
				fmt.Fprintf(file, "%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
					m.SeqN, m.MsgID, m.T1, m.T2, m.T3, m.T4, m.MessageSize,
					m.ClockOffsetMs, m.NetworkDelayMs, m.ProcessingDelayMs, m.TotalDelayMs)
			}
			fmt.Printf("Results saved to: %s\n", *output)
		}
	}

	fmt.Printf("Network monitoring data: %s\n", networkCSV)
}