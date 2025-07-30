package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
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
	numChannels     int
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
func NewPubSubProducer(nodeID string, dataPort, vizPort int, vizIP string, controlPort, numChannels int) *PubSubProducer {
	return &PubSubProducer{
		nodeID:          nodeID,
		dataPort:        dataPort,
		vizPort:         vizPort,
		vizIP:           vizIP,
		controlPort:     controlPort,
		numChannels:     numChannels,
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

// MessageTemplate holds pre-allocated message with known timestamp offset
type MessageTemplate struct {
	Data            []byte
	TimestampOffset int
}

// prepareMessages creates pre-allocated message templates with fixed-width timestamp slots
func (p *PubSubProducer) prepareMessages(totalMessages, msgSize int, rateHz float64, useCache bool) []MessageTemplate {
	dataPayload := strings.Repeat("x", msgSize)
	
	if useCache {
		// DAQ-style caching approach
		cacheFile := fmt.Sprintf("producer_cache_%.0fhz_%dmsg_%db.json", rateHz, totalMessages, msgSize)
		
		// Try to load from cache file
		if data, err := os.ReadFile(cacheFile); err == nil {
			var cached []string
			if json.Unmarshal(data, &cached) == nil && len(cached) == totalMessages {
				fmt.Printf("Loaded %d messages from cache: %s\n", totalMessages, cacheFile)
				messages := make([]MessageTemplate, totalMessages)
				for i, msg := range cached {
					msgBytes := []byte(msg)
					// Calculate timestamp offset by finding "t1="
					offset := findTimestampOffset(msgBytes)
					messages[i] = MessageTemplate{Data: msgBytes, TimestampOffset: offset}
				}
				return messages
			}
		}
		
		// Cache miss - generate and save
		messages := make([]MessageTemplate, totalMessages)
		cached := make([]string, totalMessages)
		
		for i := 0; i < totalMessages; i++ {
			template := fmt.Sprintf("request|seq_n=%d|msg_id=%d|t1=%020d|data=%s", 
				i, i, 0, dataPayload)
			msgBytes := []byte(template)
			offset := findTimestampOffset(msgBytes)
			messages[i] = MessageTemplate{Data: msgBytes, TimestampOffset: offset}
			cached[i] = template
		}
		
		// Save to cache file
		if data, err := json.Marshal(cached); err == nil {
			os.WriteFile(cacheFile, data, 0644)
			fmt.Printf("Saved %d messages to cache: %s\n", totalMessages, cacheFile)
		}
		
		return messages
	} else {
		// Non-cached approach - generate fresh messages
		messages := make([]MessageTemplate, totalMessages)
		
		for i := 0; i < totalMessages; i++ {
			template := fmt.Sprintf("request|seq_n=%d|msg_id=%d|t1=%020d|data=%s", 
				i, i, 0, dataPayload)
			msgBytes := []byte(template)
			offset := findTimestampOffset(msgBytes)
			messages[i] = MessageTemplate{Data: msgBytes, TimestampOffset: offset}
		}
		
		return messages
	}
}

// findTimestampOffset calculates the byte offset of the timestamp field once during preparation
func findTimestampOffset(message []byte) int {
	// Find "t1=" and return the offset of the timestamp data (after "t1=")
	for i := 0; i < len(message)-23; i++ {
		if string(message[i:i+3]) == "t1=" {
			return i + 3 // Return offset after "t1="
		}
	}
	return -1 // Not found
}

// setupNetworking initializes ZMQ sockets and network monitoring
func (p *PubSubProducer) setupNetworking(iface string, durationSec int) (*zmq.Socket, []*zmq.Socket, *zmq.Socket, *exec.Cmd, string, error) {
	// Control socket (REP)
	controlSocket, err := zmq.NewSocket(zmq.REP)
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	err = controlSocket.SetLinger(0) // Don't wait on close
	if err != nil {
		return nil, nil, nil, nil, "", err
	}
	err = controlSocket.Bind(fmt.Sprintf("tcp://*:%d", p.controlPort))
	if err != nil {
		return nil, nil, nil, nil, "", fmt.Errorf("failed to bind control port %d: %v", p.controlPort, err)
	}

	// Create multiple publisher sockets (one per channel)
	pubSockets := make([]*zmq.Socket, p.numChannels)
	for i := 0; i < p.numChannels; i++ {
		pubSocket, err := zmq.NewSocket(zmq.PUB)
		if err != nil {
			// Cleanup previously created sockets
			for j := 0; j < i; j++ {
				pubSockets[j].Close()
			}
			return nil, nil, nil, nil, "", err
		}
		
		err = pubSocket.SetSndhwm(10000)
		if err != nil {
			pubSocket.Close()
			return nil, nil, nil, nil, "", err
		}
		
		err = pubSocket.SetLinger(0) // Don't wait on close
		if err != nil {
			pubSocket.Close()
			return nil, nil, nil, nil, "", err
		}
		
		port := p.dataPort + i
		err = pubSocket.Bind(fmt.Sprintf("tcp://*:%d", port))
		if err != nil {
			pubSocket.Close()
			return nil, nil, nil, nil, "", fmt.Errorf("failed to bind data port %d: %v (try different port or wait a moment)", port, err)
		}
		
		pubSockets[i] = pubSocket
		fmt.Printf("Producer: Channel %d bound to port %d\n", i, port)
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
	monitorProc := exec.Command("python3", "netmonitor.py", 
		"-i", iface, "-d", strconv.Itoa(durationSec+10), "-o", networkCSV)
	monitorProc.Start()

	time.Sleep(200 * time.Millisecond)
	return controlSocket, pubSockets, vizSocket, monitorProc, networkCSV, nil
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

// updateTimestampOptimized updates timestamp using pre-calculated offset - O(1) operation
func updateTimestampOptimized(message []byte, timestampOffset int, timestamp int64) {
	// Pre-format timestamp as 20-digit string with leading zeros
	timestampStr := fmt.Sprintf("%020d", timestamp)
	
	// Direct copy to known offset - O(1) instead of O(m)
	copy(message[timestampOffset:timestampOffset+20], []byte(timestampStr))
}

// streamMessages sends messages at specified rate with timing analysis
func (p *PubSubProducer) streamMessages(pubSockets []*zmq.Socket, preparedMessages []MessageTemplate, 
	rateHz float64, durationSec int, msgSize int, debug bool) map[string]interface{} {
	
	startTime := time.Now()
	interval := time.Duration(float64(time.Second) / rateHz)
	nextSendTime := time.Now()
	seqN := 0
	sentCount := 0
	skipped := 0
	
	// Timing arrays for statistical analysis
	prepTimes := make([]float64, 0, len(preparedMessages))
	sendTimes := make([]float64, 0, len(preparedMessages))
	sleepTimes := make([]float64, 0, len(preparedMessages))
	scheduleDelays := make([]float64, 0, len(preparedMessages))
	
	for seqN < len(preparedMessages) && time.Since(startTime).Seconds() < float64(durationSec) {
		loopStart := time.Now()
		msgID := strconv.Itoa(seqN)
		t1 := timestampUs()
		template := &preparedMessages[seqN]
		
		// Check scheduling accuracy
		scheduleDelay := time.Since(nextSendTime).Seconds() * 1e6
		if debug && scheduleDelay > 100 { // > 100μs delay
			fmt.Printf("DEBUG [%d]: Schedule delay: %.0fμs\n", seqN, scheduleDelay)
		}
		scheduleDelays = append(scheduleDelays, scheduleDelay)
		
		// Message preparation timing
		prepStart := time.Now()
		updateTimestampOptimized(template.Data, template.TimestampOffset, t1)
		prepTime := time.Since(prepStart).Seconds() * 1e6
		prepTimes = append(prepTimes, prepTime)
		
		// Mutex timing
		mutexStart := time.Now()
		p.mu.Lock()
		p.pendingMessages[msgID] = PendingMessage{
			SeqN:    seqN,
			T1:      t1,
			MsgSize: msgSize,
		}
		p.mu.Unlock()
		mutexTime := time.Since(mutexStart).Seconds() * 1e6
		
		if debug && mutexTime > 50 { // > 50μs mutex time
			fmt.Printf("DEBUG [%d]: Mutex delay: %.0fμs\n", seqN, mutexTime)
		}
		
		// ZMQ send timing
		sendStart := time.Now()
		channelID := seqN % len(pubSockets)
		_, err := pubSockets[channelID].SendMessage("request", template.Data)
		sendTime := time.Since(sendStart).Seconds() * 1e6
		sendTimes = append(sendTimes, sendTime)
		
		if debug && sendTime > 100 { // > 100μs send time
			fmt.Printf("DEBUG [%d]: ZMQ send delay: %.0fμs (channel %d)\n", seqN, sendTime, channelID)
		}
		
		if err != nil {
			skipped++
			p.mu.Lock()
			delete(p.pendingMessages, msgID)
			p.mu.Unlock()
			if debug || seqN%100 == 0 {
				fmt.Printf("ZMQ queue full! Skipped %d/%d messages (%.1f%%)\n", 
					skipped, seqN, float64(skipped)/float64(seqN)*100)
			}
		} else {
			sentCount++
		}
		
		// Sleep timing
		seqN++
		nextSendTime = nextSendTime.Add(interval)
		sleepTime := time.Until(nextSendTime)
		sleepStart := time.Now()
		
		if sleepTime > 0 {
			time.Sleep(sleepTime)
			actualSleepTime := time.Since(sleepStart).Seconds() * 1e6
			sleepTimes = append(sleepTimes, actualSleepTime)
			
			if debug && math.Abs(actualSleepTime - sleepTime.Seconds()*1e6) > 100 {
				fmt.Printf("DEBUG [%d]: Sleep inaccuracy: wanted %.0fμs, got %.0fμs\n", 
					seqN-1, sleepTime.Seconds()*1e6, actualSleepTime)
			}
		} else {
			sleepTimes = append(sleepTimes, 0)
			behindMs := -sleepTime.Seconds() * 1000
			currentRate := float64(sentCount) / time.Since(startTime).Seconds()
			
			if debug || seqN%1000 == 0 {
				fmt.Printf("WARNING: %.1fms behind, rate: %.1fHz (target: %.1fHz)\n", 
					behindMs, currentRate, rateHz)
			}
		}
		
		// Overall loop timing
		loopTime := time.Since(loopStart).Seconds() * 1e6
		if debug && loopTime > 500 { // > 500μs total loop time
			fmt.Printf("DEBUG [%d]: Slow loop: %.0fμs total (prep:%.0f, mutex:%.0f, send:%.0f)\n", 
				seqN-1, loopTime, prepTime, mutexTime, sendTime)
		}
		
		// Progress reporting
		if debug && seqN%1000 == 0 {
			elapsed := time.Since(startTime).Seconds()
			rate := float64(sentCount) / elapsed
			recentDelays := scheduleDelays[max(0, len(scheduleDelays)-1000):]
			avgDelay := mean(recentDelays)
			fmt.Printf("PROGRESS [%d]: %.1fs elapsed, %.1f Hz, %.0fμs avg schedule delay\n", 
				seqN, elapsed, rate, avgDelay)
		}
	}
	
	return map[string]interface{}{
		"seq_n":          seqN,
		"sent_count":     sentCount,
		"skipped":        skipped,
		"elapsed_time":   time.Since(startTime).Seconds(),
		"prep_times":     prepTimes,
		"send_times":     sendTimes,
		"sleep_times":    sleepTimes,
		"schedule_delays": scheduleDelays,
	}
}

// analyzePerformance prints detailed performance statistics
func (p *PubSubProducer) analyzePerformance(results map[string]interface{}, rateHz float64, msgSize int, preparedMessages []MessageTemplate, debug bool) {
	seqN := results["seq_n"].(int)
	sentCount := results["sent_count"].(int) 
	skipped := results["skipped"].(int)
	elapsedTime := results["elapsed_time"].(float64)
	prepTimes := results["prep_times"].([]float64)
	
	throughputGbps := float64(sentCount*msgSize*8) / (elapsedTime * 1e9)
	
	fmt.Printf("\n=== Producer Results ===\n")
	fmt.Printf("Messages: %d/%d sent (%d skipped = %.1f%%)\n", 
		sentCount, seqN, skipped, float64(skipped)/float64(seqN)*100)
	fmt.Printf("Rate: %.1f Hz (target: %.1f Hz) | Throughput: %.2f Gbps\n", 
		float64(sentCount)/elapsedTime, rateHz, throughputGbps)
	
	// Statistical analysis of preparation times (only in debug mode)
	if debug && len(prepTimes) > 0 {
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

// waitForResponses waits for all sent messages to receive responses
func (p *PubSubProducer) waitForResponses(expectedResponses int, debug bool) {
	if expectedResponses == 0 {
		return
	}
	
	maxWaitTime := 30 * time.Second // Maximum time to wait
	checkInterval := 500 * time.Millisecond
	startWait := time.Now()
	
	if debug {
		fmt.Printf("Waiting for responses: expecting %d messages\n", expectedResponses)
	}
	
	for time.Since(startWait) < maxWaitTime {
		p.mu.RLock()
		pendingCount := len(p.pendingMessages)
		receivedCount := len(p.measurements)
		p.mu.RUnlock()
		
		if debug && time.Since(startWait).Seconds() > 1 { // Report every check after 1s
			fmt.Printf("Response status: %d received, %d pending (%.1f%% complete)\n", 
				receivedCount, pendingCount, float64(receivedCount)/float64(expectedResponses)*100)
		}
		
		// Check if we've received responses for all sent messages
		if receivedCount >= expectedResponses {
			if debug {
				fmt.Printf("✅ All %d responses received in %.1fs\n", 
					receivedCount, time.Since(startWait).Seconds())
			}
			return
		}
		
		// Also check if no more pending messages (alternative completion condition)
		if pendingCount == 0 && receivedCount > 0 {
			if debug {
				fmt.Printf("✅ No pending messages, received %d responses in %.1fs\n", 
					receivedCount, time.Since(startWait).Seconds())
			}
			return
		}
		
		time.Sleep(checkInterval)
	}
	
	// Timeout reached
	p.mu.RLock()
	finalReceived := len(p.measurements)
	finalPending := len(p.pendingMessages)
	p.mu.RUnlock()
	
	fmt.Printf("⚠️  Response timeout after %.1fs: %d/%d received (%d still pending)\n", 
		maxWaitTime.Seconds(), finalReceived, expectedResponses, finalPending)
}

// analyzeNetworkCSV calls the Python network analysis script
func (p *PubSubProducer) analyzeNetworkCSV(csvFile string, expectedGbps float64) {
	cmd := exec.Command("python3", "analyze_netmonitor.py", csvFile, 
		"--expected-gbps", fmt.Sprintf("%.2f", expectedGbps))
	
	output, err := cmd.Output()
	if err != nil {
		fmt.Printf("Network analysis failed: %v\n", err)
		return
	}
	
	fmt.Print(string(output))
}

// runProducer executes the main producer logic
func (p *PubSubProducer) runProducer(rateHz float64, durationSec, msgSize int, iface string, useCache, debug bool) (string, error) {
	controlSocket, pubSockets, vizSocket, monitorProc, networkCSV, err := p.setupNetworking(iface, durationSec)
	if err != nil {
		return "", err
	}
	
	// Cleanup all sockets
	defer func() {
		for _, socket := range pubSockets {
			socket.Close()
		}
		vizSocket.Close()
	}()

	totalMessages := int(rateHz * float64(durationSec))
	preparedMessages := p.prepareMessages(totalMessages, msgSize, rateHz, useCache)
	
	err = p.waitForConsumer(controlSocket)
	if err != nil {
		return "", err
	}
	
	p.streamingStarted = true
	results := p.streamMessages(pubSockets, preparedMessages, rateHz, durationSec, msgSize, debug)
	p.analyzePerformance(results, rateHz, msgSize, preparedMessages, debug)
	
	// Wait for all responses to arrive
	sentCount := results["sent_count"].(int)
	p.waitForResponses(sentCount, debug)
	
	time.Sleep(2 * time.Second) // Final buffer
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

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func main() {
	// Command line flags
	rate := flag.Float64("r", 10.0, "Message rate Hz")
	duration := flag.Int("t", 60, "Duration seconds")
	msgSize := flag.Int("s", 1024, "Message size in bytes")
	dataPort := flag.Int("data-port", 5555, "Producer publisher port")
	vizPort := flag.Int("viz-port", 5570, "Client publisher port")
	controlPort := flag.Int("control-port", 5580, "Control channel port")
	vizIP := flag.String("viz-ip", "localhost", "Visualization IP address")
	iface := flag.String("i", "lo0", "Network interface for monitoring")
	output := flag.String("output", "producer_results.csv", "Output CSV filename")
	cache := flag.Bool("cache", false, "Use DAQ-style message caching")
	numChannels := flag.Int("channels", 1, "Number of ZMQ channels")
	debugFlag := flag.Bool("d", false, "Enable debug output")
	flag.Parse()
	
	if *debugFlag {
		fmt.Printf("Debug mode enabled - detailed timing and response tracking\n")
	}

	fmt.Printf("Go Pub-Sub Producer v2.0 - Multi-Channel\n")
	fmt.Printf("Channels: %d | Rate: %.1f Hz | Duration: %ds | Message Size: %d bytes\n", *numChannels, *rate, *duration, *msgSize)

	producer := NewPubSubProducer("producer", *dataPort, *vizPort, *vizIP, *controlPort, *numChannels)
	networkCSV, err := producer.runProducer(*rate, *duration, *msgSize, *iface, *cache, *debugFlag)
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

		fmt.Printf("\n=== Process Results ===\n")
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

	// Analyze network data using Python script
	// Calculate expected Gbps from rate and message size
	expectedGbps := (float64(*rate) * float64(*msgSize) * 8) / 1e9
	producer.analyzeNetworkCSV(networkCSV, expectedGbps)
	fmt.Printf("Network monitoring data: %s\n", networkCSV)
}
