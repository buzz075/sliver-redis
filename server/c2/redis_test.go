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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	implantCrypto "github.com/bishopfox/sliver/implant/sliver/cryptography"
	"github.com/bishopfox/sliver/protobuf/clientpb"
	"github.com/bishopfox/sliver/protobuf/sliverpb"
	"github.com/bishopfox/sliver/server/cryptography"
	"github.com/bishopfox/sliver/server/db"
	"github.com/bishopfox/sliver/server/db/models"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"
)

// TestRedisSessionID tests session ID generation
func TestRedisSessionID(t *testing.T) {
	sessionIDs := make(map[string]bool)

	// Generate 1000 session IDs and verify uniqueness
	for i := 0; i < 1000; i++ {
		id := newRedisSessionID()

		// Check it's 32 characters (16 bytes hex encoded)
		if len(id) != 32 {
			t.Fatalf("Expected session ID length 32, got %d", len(id))
		}

		// Check it's valid hex
		_, err := hex.DecodeString(id)
		if err != nil {
			t.Fatalf("Session ID is not valid hex: %s", err)
		}

		// Check uniqueness
		if sessionIDs[id] {
			t.Fatalf("Duplicate session ID generated: %s", id)
		}
		sessionIDs[id] = true
	}
}

// TestRedisListener tests starting and stopping a Redis listener
func TestRedisListener(t *testing.T) {
	// Start miniredis server
	mr := miniredis.RunT(t)
	defer mr.Close()

	// Parse address
	portInt, _ := strconv.Atoi(mr.Port())
	host, port := "127.0.0.1", uint32(portInt)

	req := &clientpb.RedisListenerReq{
		Host: host,
		Port: port,
	}

	// Start Redis listener
	listener, err := StartRedisListener(req)
	if err != nil {
		t.Fatalf("Failed to start Redis listener: %s", err)
	}
	defer listener.Cleanup()

	// Verify listener is running
	if listener.ServerConf.Port != port {
		t.Fatalf("Expected port %d, got %d", port, listener.ServerConf.Port)
	}

	// Verify can connect to miniredis
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = listener.client.Ping(ctx).Err()
	if err != nil {
		t.Fatalf("Failed to ping Redis: %s", err)
	}
}

// TestRedisSessionManagement tests session creation and removal
func TestRedisSessionManagement(t *testing.T) {
	sessions := &RedisSessions{
		active: make(map[string]*RedisSession),
		mutex:  &sync.RWMutex{},
	}

	// Create test sessions
	session1 := &RedisSession{ID: "session1"}
	session2 := &RedisSession{ID: "session2"}

	// Add sessions
	sessions.Add(session1)
	sessions.Add(session2)

	// Verify retrieval
	retrieved := sessions.Get("session1")
	if retrieved == nil || retrieved.ID != "session1" {
		t.Fatal("Failed to retrieve session1")
	}

	// Verify All()
	all := sessions.All()
	if len(all) != 2 {
		t.Fatalf("Expected 2 sessions, got %d", len(all))
	}

	// Remove session
	sessions.Remove("session1")
	if sessions.Get("session1") != nil {
		t.Fatal("Session1 should have been removed")
	}

	// Verify count
	all = sessions.All()
	if len(all) != 1 {
		t.Fatalf("Expected 1 session after removal, got %d", len(all))
	}
}

// TestRedisSessionInit tests the session initialization flow
func TestRedisSessionInit(t *testing.T) {
	// Start miniredis server
	mr := miniredis.RunT(t)
	defer mr.Close()

	// Setup cryptographic keys
	serverKeyPair := cryptography.AgeServerKeyPair()
	peerKeyPair, _ := cryptography.RandomAgeKeyPair()

	implantCrypto.SetSecrets(
		peerKeyPair.Public,
		peerKeyPair.Private,
		"",
		serverKeyPair.Public,
		cryptography.MinisignServerPublicKey(),
	)

	// Create implant build in database
	digest := sha256.New()
	digest.Write([]byte(peerKeyPair.Public))
	publicKeyDigest := hex.EncodeToString(digest.Sum(nil))

	// Generate unique name to avoid UNIQUE constraint violation
	buildName := fmt.Sprintf("test-redis-build-%d", time.Now().UnixNano())

	implantBuild := &models.ImplantBuild{
		Name:                buildName,
		PeerPublicKey:       peerKeyPair.Public,
		PeerPublicKeyDigest: publicKeyDigest,
		PeerPrivateKey:      peerKeyPair.Private,
		AgeServerPublicKey:  serverKeyPair.Public,
	}
	err := db.Session().Create(implantBuild).Error
	if err != nil {
		t.Fatalf("Failed to create implant build: %s", err)
	}
	defer db.Session().Delete(implantBuild)

	// Start Redis listener
	portInt, _ := strconv.Atoi(mr.Port())
	req := &clientpb.RedisListenerReq{
		Host: "127.0.0.1",
		Port: uint32(portInt),
	}

	listener, err := StartRedisListener(req)
	if err != nil {
		t.Fatalf("Failed to start Redis listener: %s", err)
	}
	defer listener.Cleanup()

	// Simulate implant session initialization
	sKey := cryptography.RandomSymmetricKey()
	sessionInit := &sliverpb.HTTPSessionInit{Key: sKey[:]}
	data, err := proto.Marshal(sessionInit)
	if err != nil {
		t.Fatalf("Failed to marshal session init: %s", err)
	}

	// Encrypt with Age key exchange
	encryptedInit, err := implantCrypto.AgeKeyExToServer(data)
	if err != nil {
		t.Fatalf("Failed to encrypt session init: %s", err)
	}

	// Push to init queue
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	initKey := "sliver:init"
	err = listener.client.RPush(ctx, initKey, encryptedInit).Err()
	if err != nil {
		t.Fatalf("Failed to push to init queue: %s", err)
	}

	// Wait for session to be created (give server time to process)
	time.Sleep(100 * time.Millisecond)

	// Verify session was created
	allSessions := listener.sessions.All()
	if len(allSessions) == 0 {
		t.Fatal("No session was created")
	}

	// Verify session has cipher context
	session := allSessions[0]
	if session.CipherCtx == nil {
		t.Fatal("Session cipher context is nil")
	}

	// Verify session ID was generated
	if session.ID == "" {
		t.Fatal("Session ID is empty")
	}
}

// TestRedisMessageEncryption tests message encryption and decryption
func TestRedisMessageEncryption(t *testing.T) {
	// Create cipher contexts - server encrypts, implant decrypts
	// This simulates the real communication pattern
	sKey := cryptography.RandomSymmetricKey()
	serverCipherCtx := cryptography.NewCipherContext(sKey)
	implantCipherCtx := implantCrypto.NewCipherContext(sKey)

	// Create test envelope
	testEnvelope := &sliverpb.Envelope{
		ID:   12345,
		Type: uint32(sliverpb.MsgPing),
		Data: []byte("test payload"),
	}

	// Serialize
	data, err := proto.Marshal(testEnvelope)
	if err != nil {
		t.Fatalf("Failed to marshal envelope: %s", err)
	}

	// Server encrypts (adds minisign signature)
	encData, err := serverCipherCtx.Encrypt(data)
	if err != nil {
		t.Fatalf("Failed to encrypt data: %s", err)
	}

	// Implant decrypts (strips signature and decrypts)
	decData, err := implantCipherCtx.Decrypt(encData)
	if err != nil {
		t.Fatalf("Failed to decrypt data: %s", err)
	}

	// Deserialize
	decEnvelope := &sliverpb.Envelope{}
	err = proto.Unmarshal(decData, decEnvelope)
	if err != nil {
		t.Fatalf("Failed to unmarshal decrypted envelope: %s", err)
	}

	// Verify
	if decEnvelope.ID != testEnvelope.ID {
		t.Fatalf("Expected envelope ID %d, got %d", testEnvelope.ID, decEnvelope.ID)
	}
	if string(decEnvelope.Data) != string(testEnvelope.Data) {
		t.Fatalf("Expected data %s, got %s", testEnvelope.Data, decEnvelope.Data)
	}
}

// TestRedisQueueOperations tests Redis queue push/pop operations
func TestRedisQueueOperations(t *testing.T) {
	// Start miniredis server
	mr := miniredis.RunT(t)
	defer mr.Close()

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	ctx := context.Background()
	queueKey := "test:queue"

	// Test RPUSH
	testData := []byte("test message")
	err := client.RPush(ctx, queueKey, testData).Err()
	if err != nil {
		t.Fatalf("RPUSH failed: %s", err)
	}

	// Verify queue length
	length, err := client.LLen(ctx, queueKey).Result()
	if err != nil {
		t.Fatalf("LLEN failed: %s", err)
	}
	if length != 1 {
		t.Fatalf("Expected queue length 1, got %d", length)
	}

	// Test BLPOP
	result, err := client.BLPop(ctx, time.Second, queueKey).Result()
	if err != nil {
		t.Fatalf("BLPOP failed: %s", err)
	}

	if len(result) != 2 {
		t.Fatalf("Expected 2 elements in BLPOP result, got %d", len(result))
	}

	if string(result[1]) != string(testData) {
		t.Fatalf("Expected data %s, got %s", testData, result[1])
	}

	// Verify queue is empty
	length, err = client.LLen(ctx, queueKey).Result()
	if err != nil {
		t.Fatalf("LLEN failed: %s", err)
	}
	if length != 0 {
		t.Fatalf("Expected queue length 0, got %d", length)
	}
}

// TestRedisSessionCleanup tests session cleanup
func TestRedisSessionCleanup(t *testing.T) {
	// Start miniredis server
	mr := miniredis.RunT(t)
	defer mr.Close()

	portInt, _ := strconv.Atoi(mr.Port())
	req := &clientpb.RedisListenerReq{
		Host: "127.0.0.1",
		Port: uint32(portInt),
	}

	listener, err := StartRedisListener(req)
	if err != nil {
		t.Fatalf("Failed to start Redis listener: %s", err)
	}
	defer listener.Cleanup()

	// Create test session
	sessionID := newRedisSessionID()
	session := &RedisSession{
		ID:            sessionID,
		upstreamKey:   fmt.Sprintf("sliver:sessions:%s:upstream", sessionID),
		downstreamKey: fmt.Sprintf("sliver:sessions:%s:downstream", sessionID),
		heartbeatKey:  fmt.Sprintf("sliver:sessions:%s:heartbeat", sessionID),
		closeKey:      fmt.Sprintf("sliver:sessions:%s:close", sessionID),
	}

	listener.sessions.Add(session)

	// Create test keys in Redis
	ctx := context.Background()
	listener.client.Set(ctx, session.upstreamKey, "test", 0)
	listener.client.Set(ctx, session.downstreamKey, "test", 0)
	listener.client.Set(ctx, session.heartbeatKey, "test", 0)

	// Verify keys exist
	exists, err := listener.client.Exists(ctx, session.upstreamKey).Result()
	if err != nil || exists != 1 {
		t.Fatal("Upstream key should exist")
	}

	// Clean up session
	listener.closeSession(session)

	// Verify session removed from active sessions
	if listener.sessions.Get(sessionID) != nil {
		t.Fatal("Session should have been removed from active sessions")
	}

	// Verify Redis keys deleted
	time.Sleep(100 * time.Millisecond) // Give time for async deletion

	exists, err = listener.client.Exists(ctx, session.upstreamKey).Result()
	if err != nil {
		t.Fatalf("Failed to check key existence: %s", err)
	}
	if exists != 0 {
		t.Fatal("Upstream key should have been deleted")
	}
}

// TestRedisHeartbeat tests heartbeat functionality
func TestRedisHeartbeat(t *testing.T) {
	// Start miniredis server
	mr := miniredis.RunT(t)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	ctx := context.Background()
	heartbeatKey := "test:heartbeat"

	// Set heartbeat with TTL
	err := client.Set(ctx, heartbeatKey, time.Now().Unix(), 60*time.Second).Err()
	if err != nil {
		t.Fatalf("Failed to set heartbeat: %s", err)
	}

	// Verify key exists
	exists, err := client.Exists(ctx, heartbeatKey).Result()
	if err != nil || exists != 1 {
		t.Fatal("Heartbeat key should exist")
	}

	// Check TTL
	ttl, err := client.TTL(ctx, heartbeatKey).Result()
	if err != nil {
		t.Fatalf("Failed to get TTL: %s", err)
	}

	if ttl < 0 {
		t.Fatal("Heartbeat key should have positive TTL")
	}

	if ttl > 60*time.Second {
		t.Fatalf("TTL should be <= 60 seconds, got %v", ttl)
	}
}

// TestRedisMultipleSessions tests handling multiple concurrent sessions
func TestRedisMultipleSessions(t *testing.T) {
	// Start miniredis server
	mr := miniredis.RunT(t)
	defer mr.Close()

	portInt, _ := strconv.Atoi(mr.Port())
	req := &clientpb.RedisListenerReq{
		Host: "127.0.0.1",
		Port: uint32(portInt),
	}

	listener, err := StartRedisListener(req)
	if err != nil {
		t.Fatalf("Failed to start Redis listener: %s", err)
	}
	defer listener.Cleanup()

	// Create multiple sessions
	numSessions := 10
	sessionIDs := make([]string, numSessions)

	for i := 0; i < numSessions; i++ {
		sessionID := newRedisSessionID()
		sessionIDs[i] = sessionID

		session := &RedisSession{
			ID:            sessionID,
			upstreamKey:   fmt.Sprintf("sliver:sessions:%s:upstream", sessionID),
			downstreamKey: fmt.Sprintf("sliver:sessions:%s:downstream", sessionID),
		}

		listener.sessions.Add(session)
	}

	// Verify all sessions are tracked
	allSessions := listener.sessions.All()
	if len(allSessions) != numSessions {
		t.Fatalf("Expected %d sessions, got %d", numSessions, len(allSessions))
	}

	// Verify each session can be retrieved
	for _, sessionID := range sessionIDs {
		session := listener.sessions.Get(sessionID)
		if session == nil {
			t.Fatalf("Failed to retrieve session %s", sessionID)
		}
	}
}

// BenchmarkRedisSessionID benchmarks session ID generation
func BenchmarkRedisSessionID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = newRedisSessionID()
	}
}

// BenchmarkRedisEncryption benchmarks message encryption
func BenchmarkRedisEncryption(b *testing.B) {
	sKey := cryptography.RandomSymmetricKey()
	cipherCtx := cryptography.NewCipherContext(sKey)

	testEnvelope := &sliverpb.Envelope{
		ID:   12345,
		Type: uint32(sliverpb.MsgPing),
		Data: make([]byte, 1024), // 1KB payload
	}

	data, _ := proto.Marshal(testEnvelope)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cipherCtx.Encrypt(data)
	}
}

// BenchmarkRedisDecryption benchmarks message decryption
func BenchmarkRedisDecryption(b *testing.B) {
	sKey := cryptography.RandomSymmetricKey()
	cipherCtx := cryptography.NewCipherContext(sKey)

	testEnvelope := &sliverpb.Envelope{
		ID:   12345,
		Type: uint32(sliverpb.MsgPing),
		Data: make([]byte, 1024), // 1KB payload
	}

	data, _ := proto.Marshal(testEnvelope)
	encData, _ := cipherCtx.Encrypt(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cipherCtx.Decrypt(encData)
	}
}
