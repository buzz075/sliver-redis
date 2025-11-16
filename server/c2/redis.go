package c2

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

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/bishopfox/sliver/protobuf/clientpb"
	"github.com/bishopfox/sliver/protobuf/sliverpb"
	"github.com/bishopfox/sliver/server/core"
	"github.com/bishopfox/sliver/server/cryptography"
	"github.com/bishopfox/sliver/server/db"
	"github.com/bishopfox/sliver/server/log"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"
)

var (
	redisLog = log.NamedLogger("c2", "redis")
)

const (
	DefaultRedisTimeout     = 30 * time.Second
	RedisInitQueue          = "sliver:init"
	RedisSessionKeyTemplate = "sliver:sessions:%s"
	RedisHeartbeatTimeout   = 120 * time.Second
)

// RedisSession - Holds data related to a Redis C2 session
type RedisSession struct {
	ID            string
	ImplantConn   *core.ImplantConnection
	CipherCtx     *cryptography.CipherContext
	Started       time.Time
	upstreamKey   string // Queue for implant -> server
	downstreamKey string // Queue for server -> implant
	heartbeatKey  string // Heartbeat tracker
	closeKey      string // Close signal
}

// RedisSessions - All currently open Redis sessions
type RedisSessions struct {
	active map[string]*RedisSession
	mutex  *sync.RWMutex
}

// Add - Add a Redis session
func (s *RedisSessions) Add(session *RedisSession) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.active[session.ID] = session
}

// Get - Get a Redis session
func (s *RedisSessions) Get(sessionID string) *RedisSession {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.active[sessionID]
}

// Remove - Remove a Redis session
func (s *RedisSessions) Remove(sessionID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.active, sessionID)
}

// All - Get all active sessions
func (s *RedisSessions) All() []*RedisSession {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	sessions := make([]*RedisSession, 0, len(s.active))
	for _, session := range s.active {
		sessions = append(sessions, session)
	}
	return sessions
}

// SliverRedisC2 - Holds refs to all the Redis C2 objects
type SliverRedisC2 struct {
	client     *redis.Client
	ServerConf *clientpb.RedisListenerReq
	sessions   *RedisSessions
	ctx        context.Context
	cancel     context.CancelFunc
	Cleanup    func()
}

// StartRedisListener - Start a Redis C2 listener
func StartRedisListener(req *clientpb.RedisListenerReq) (*SliverRedisC2, error) {
	redisLog.Infof("Starting Redis listener on '%s:%d'", req.Host, req.Port)

	ctx, cancel := context.WithCancel(context.Background())

	redisOpts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", req.Host, req.Port),
		Password: req.Password,
		DB:       int(req.DB),
	}

	// TODO: Add TLS support if req.TLSEnabled

	client := redis.NewClient(redisOpts)

	// Test connection
	pingCtx, pingCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pingCancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		redisLog.Errorf("Failed to connect to Redis: %v", err)
		cancel()
		return nil, err
	}

	server := &SliverRedisC2{
		client:     client,
		ServerConf: req,
		ctx:        ctx,
		cancel:     cancel,
		sessions: &RedisSessions{
			active: make(map[string]*RedisSession),
			mutex:  &sync.RWMutex{},
		},
	}

	server.Cleanup = func() {
		redisLog.Info("Cleaning up Redis listener")
		cancel()

		// Close all active sessions
		for _, session := range server.sessions.All() {
			server.closeSession(session)
		}

		client.Close()
	}

	// Start session initialization handler
	go server.handleSessionInit()

	// Start session monitor (cleanup dead sessions)
	go server.monitorSessions()

	redisLog.Infof("Redis listener started successfully")
	return server, nil
}

// handleSessionInit - Listen for new session initialization requests
func (s *SliverRedisC2) handleSessionInit() {
	redisLog.Info("Starting session init handler")

	for {
		select {
		case <-s.ctx.Done():
			redisLog.Info("Session init handler stopped")
			return
		default:
			// Block waiting for init request (1 second timeout to allow checking ctx.Done())
			result, err := s.client.BLPop(s.ctx, time.Second, RedisInitQueue).Result()
			if err != nil {
				if err != redis.Nil && err != context.Canceled {
					redisLog.Debugf("Init queue error: %v", err)
				}
				continue
			}

			if len(result) < 2 {
				redisLog.Warn("Invalid init queue result")
				continue
			}

			encryptedInit := []byte(result[1])
			go s.processSessionInit(encryptedInit)
		}
	}
}

// processSessionInit - Handle a single session initialization request
func (s *SliverRedisC2) processSessionInit(encryptedInit []byte) {
	// The first 32 bytes are the public key digest
	if len(encryptedInit) < 32 {
		redisLog.Warn("Invalid data length")
		return
	}

	var publicKeyDigest [32]byte
	copy(publicKeyDigest[:], encryptedInit[:32])
	implantBuild, err := db.ImplantBuildByPublicKeyDigest(publicKeyDigest)
	if err != nil || implantBuild == nil {
		redisLog.Warn("Unknown public key")
		return
	}

	// Decrypt using Age key exchange
	serverKeyPair := cryptography.AgeServerKeyPair()
	sessionInitData, err := cryptography.AgeKeyExFromImplant(serverKeyPair.Private, implantBuild.PeerPrivateKey, encryptedInit[32:])
	if err != nil {
		redisLog.Errorf("Failed to decrypt session init: %v", err)
		return
	}

	// Parse session init message
	sessionInit := &sliverpb.HTTPSessionInit{} // Reuse HTTPSessionInit protobuf
	if err := proto.Unmarshal(sessionInitData, sessionInit); err != nil {
		redisLog.Errorf("Failed to unmarshal session init: %v", err)
		return
	}

	// Create cipher context from provided symmetric key
	sKey, err := cryptography.KeyFromBytes(sessionInit.Key)
	if err != nil {
		redisLog.Errorf("Failed to convert bytes to session key: %v", err)
		return
	}
	cipherCtx := cryptography.NewCipherContext(sKey)

	// Generate unique session ID
	sessionID := newRedisSessionID()

	// Create implant connection
	implantConn := core.NewImplantConnection("redis", sessionID)
	if implantConn == nil {
		redisLog.Error("Failed to create implant connection")
		return
	}

	// Create session object
	session := &RedisSession{
		ID:            sessionID,
		ImplantConn:   implantConn,
		CipherCtx:     cipherCtx,
		Started:       time.Now(),
		upstreamKey:   fmt.Sprintf("sliver:sessions:%s:upstream", sessionID),
		downstreamKey: fmt.Sprintf("sliver:sessions:%s:downstream", sessionID),
		heartbeatKey:  fmt.Sprintf("sliver:sessions:%s:heartbeat", sessionID),
		closeKey:      fmt.Sprintf("sliver:sessions:%s:close", sessionID),
	}

	// Store session
	s.sessions.Add(session)

	// Encrypt session ID and send back to implant
	encryptedSessionID, err := cipherCtx.Encrypt([]byte(sessionID))
	if err != nil {
		redisLog.Errorf("Failed to encrypt session ID: %v", err)
		s.sessions.Remove(sessionID)
		return
	}

	// Send to response queue
	// Note: In production, you'd need a better addressing scheme
	// For now, we push to a response queue that the implant is listening on
	responseKey := fmt.Sprintf("sliver:init:response:%s", s.ServerConf.Host)
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	if err := s.client.RPush(ctx, responseKey, encryptedSessionID).Err(); err != nil {
		redisLog.Errorf("Failed to send session ID response: %v", err)
		cancel()
		s.sessions.Remove(sessionID)
		return
	}
	cancel()

	redisLog.Infof("New Redis session established: %s", sessionID)

	// Start handlers for this session
	go s.handleUpstream(session)
	go s.handleDownstream(session)
	go s.monitorSessionHealth(session)
}

// handleUpstream - Read messages from implant and forward to core
func (s *SliverRedisC2) handleUpstream(session *RedisSession) {
	redisLog.Debugf("Starting upstream handler for session %s", session.ID)

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			// Check if session should be closed
			closeVal, _ := s.client.Get(s.ctx, session.closeKey).Result()
			if closeVal == "1" {
				redisLog.Infof("Session %s close requested", session.ID)
				s.closeSession(session)
				return
			}

			// Block waiting for message from implant (1 second timeout)
			result, err := s.client.BLPop(s.ctx, time.Second, session.upstreamKey).Result()
			if err != nil {
				if err != redis.Nil && err != context.Canceled {
					redisLog.Debugf("Upstream read error for %s: %v", session.ID, err)
				}
				continue
			}

			if len(result) < 2 {
				continue
			}

			encData := []byte(result[1])

			// Decrypt message
			data, err := session.CipherCtx.Decrypt(encData)
			if err != nil {
				redisLog.Errorf("Failed to decrypt upstream message: %v", err)
				continue
			}

			// Deserialize envelope
			envelope := &sliverpb.Envelope{}
			if err := proto.Unmarshal(data, envelope); err != nil {
				redisLog.Errorf("Failed to unmarshal envelope: %v", err)
				continue
			}

			// Forward to implant connection for processing
			session.ImplantConn.Send <- envelope
		}
	}
}

// handleDownstream - Send messages from core to implant
func (s *SliverRedisC2) handleDownstream(session *RedisSession) {
	redisLog.Debugf("Starting downstream handler for session %s", session.ID)

	for {
		select {
		case <-s.ctx.Done():
			return
		case envelope, ok := <-session.ImplantConn.Send:
			if !ok {
				// Channel closed, session ending
				redisLog.Infof("Session %s send channel closed", session.ID)
				s.closeSession(session)
				return
			}

			// Serialize envelope
			data, err := proto.Marshal(envelope)
			if err != nil {
				redisLog.Errorf("Failed to marshal envelope: %v", err)
				continue
			}

			// Encrypt message
			encData, err := session.CipherCtx.Encrypt(data)
			if err != nil {
				redisLog.Errorf("Failed to encrypt downstream message: %v", err)
				continue
			}

			// Push to downstream queue for implant to read
			ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
			if err := s.client.RPush(ctx, session.downstreamKey, encData).Err(); err != nil {
				redisLog.Errorf("Failed to push downstream message: %v", err)
				cancel()
				continue
			}
			cancel()
		}
	}
}

// monitorSessionHealth - Monitor session heartbeat
func (s *SliverRedisC2) monitorSessionHealth(session *RedisSession) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Check heartbeat key TTL
			ttl, err := s.client.TTL(s.ctx, session.heartbeatKey).Result()
			if err != nil || ttl < 0 {
				// Heartbeat expired or missing
				redisLog.Warnf("Session %s heartbeat expired", session.ID)
				s.closeSession(session)
				return
			}
		}
	}
}

// monitorSessions - Periodically clean up dead sessions
func (s *SliverRedisC2) monitorSessions() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	redisLog.Info("Starting session monitor")

	for {
		select {
		case <-s.ctx.Done():
			redisLog.Info("Session monitor stopped")
			return
		case <-ticker.C:
			sessions := s.sessions.All()
			for _, session := range sessions {
				// Check if implant connection is still active
				if session.ImplantConn == nil {
					redisLog.Infof("Cleaning up session with nil connection: %s", session.ID)
					s.closeSession(session)
					continue
				}

				// Check session age (cleanup very old sessions)
				if time.Since(session.Started) > 24*time.Hour {
					redisLog.Infof("Cleaning up old session: %s", session.ID)
					s.closeSession(session)
				}
			}
		}
	}
}

// closeSession - Clean up a session
func (s *SliverRedisC2) closeSession(session *RedisSession) {
	redisLog.Infof("Closing session: %s", session.ID)

	// Remove from active sessions
	s.sessions.Remove(session.ID)

	// Clean up Redis keys
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.client.Del(ctx, session.upstreamKey)
	s.client.Del(ctx, session.downstreamKey)
	s.client.Del(ctx, session.heartbeatKey)
	s.client.Del(ctx, session.closeKey)

	// Cleanup implant connection
	if session.ImplantConn != nil && session.ImplantConn.Cleanup != nil {
		session.ImplantConn.Cleanup()
	}
}

// newRedisSessionID - Generate a 128-bit session ID
func newRedisSessionID() string {
	buf := make([]byte, 16)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}
