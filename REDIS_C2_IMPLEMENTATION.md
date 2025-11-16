# Redis C2 Transport Implementation

This document describes the Redis-based communication pathway implementation for Sliver C2 framework.

## Overview

The Redis transport uses Redis queues (Lists) to mimic HTTP-like network connections, providing a queue-based alternative to traditional HTTP/HTTPS C2 communication. This implementation follows the same architecture patterns as the existing HTTP transport.

## Architecture

### Design Philosophy

The Redis transport implements the same **Connection abstraction** used by all Sliver transports:
- **Channel-based I/O**: Uses Go channels for sending/receiving protobuf Envelopes
- **Encrypted sessions**: ChaCha20Poly1305 encryption with Age X25519 key exchange
- **Long polling semantics**: BLPOP (blocking list pop) mimics HTTP long polling
- **Session management**: Server tracks active sessions with heartbeat monitoring

### Queue Structure

Each session uses dedicated Redis queues:

```
Session Initialization:
  sliver:init                              - Global queue for new session requests
  sliver:init:response:{host}              - Response queue for session IDs

Active Session (per session ID):
  sliver:sessions:{sessionID}:upstream     - Implant → Server messages
  sliver:sessions:{sessionID}:downstream   - Server → Implant messages
  sliver:sessions:{sessionID}:heartbeat    - Session keepalive (TTL-based)
  sliver:sessions:{sessionID}:close        - Close signal
```

## Implementation Files

### Implant Side (Client)

**`implant/sliver/transports/redisclient/redisclient.go`**
- `SliverRedisClient` - Main Redis client structure
- `RedisStartSession()` - Initialize connection and session
- `SessionInit()` - Perform Age key exchange with server
- `WriteEnvelope()` - Encrypt and push messages to upstream queue
- `ReadEnvelope()` - Block-pop from downstream queue (with timeout)
- `heartbeat()` - Periodic session keepalive

**`implant/sliver/transports/session.go`** (additions)
- `redisConnect()` - Creates Connection for redis:// URIs
- `parseRedisOptions()` - Parses Redis-specific query parameters

### Server Side (C2)

**`server/c2/redis.go`**
- `SliverRedisC2` - Main Redis C2 listener
- `StartRedisListener()` - Initialize Redis listener and handlers
- `handleSessionInit()` - Process new session initialization requests
- `handleUpstream()` - Read implant messages from upstream queue
- `handleDownstream()` - Send server messages to downstream queue
- `monitorSessionHealth()` - Check heartbeat for individual session
- `monitorSessions()` - Periodic cleanup of dead sessions

### Protocol Definitions

**`protobuf/clientpb/client.proto`** (additions)
```protobuf
message RedisListenerReq {
  string Host = 1;
  uint32 Port = 2;
  string Password = 3;
  uint32 DB = 4;
  bool TLSEnabled = 5;
  bool EnforceOTP = 6;
}
```

**`protobuf/rpcpb/services.proto`** (additions)
```protobuf
rpc StartRedisListener(clientpb.RedisListenerReq) returns (clientpb.ListenerJob);
```

## Connection Flow

### 1. Session Initialization

```
Implant:
  1. Generate random symmetric key
  2. Create HTTPSessionInit protobuf with key (reuses existing message)
  3. Age-encrypt using server's public key
  4. RPUSH encrypted data to sliver:init
  5. BLPOP from sliver:init:response:{host} (wait for session ID)
  6. Decrypt session ID with symmetric key
  7. Set up queue keys using session ID

Server:
  1. BLPOP from sliver:init (wait for init requests)
  2. Age-decrypt using server private key
  3. Extract symmetric key
  4. Generate unique session ID
  5. Create CipherContext with key
  6. Encrypt session ID
  7. RPUSH encrypted session ID to response queue
  8. Create ImplantConnection
  9. Start upstream/downstream/heartbeat handlers
```

### 2. Normal Operation

```
Sending (Implant → Server):
  Implant: proto.Marshal(envelope) → Encrypt → RPUSH to upstream
  Server:  BLPOP from upstream → Decrypt → Unmarshal → Forward to core

Receiving (Server → Implant):
  Server:  Marshal → Encrypt → RPUSH to downstream
  Implant: BLPOP from downstream → Decrypt → Unmarshal → Process
```

### 3. Heartbeat

```
Implant: Every 30s: SET heartbeat key with 60s TTL
Server:  Every 30s: Check TTL on heartbeat key
         If TTL < 0: Session dead, cleanup
```

### 4. Cleanup

```
Implant: SET close key = "1" with 60s TTL
Server:  Detect close key, cleanup session
         DELETE all session keys
         Remove ImplantConnection
```

## Configuration

### URI Format

```
redis://host:port?param1=value1&param2=value2
```

### Query Parameters

| Parameter      | Type     | Default | Description                              |
|---------------|----------|---------|------------------------------------------|
| password      | string   | ""      | Redis password                           |
| db            | int      | 0       | Redis database number                    |
| poll-timeout  | duration | 30s     | BLPOP timeout (long polling)            |
| dial-timeout  | duration | 10s     | Connection dial timeout                  |
| read-timeout  | duration | 30s     | Socket read timeout                      |
| write-timeout | duration | 10s     | Socket write timeout                     |
| max-errors    | int      | 10      | Max consecutive errors before reconnect  |
| tls           | bool     | false   | Enable TLS (TODO: not implemented)      |

### Example URIs

```bash
# Basic connection
redis://localhost:6379

# With password and custom database
redis://localhost:6379?password=secret&db=1

# Custom timeouts
redis://localhost:6379?poll-timeout=60s&dial-timeout=15s

# Production setup
redis://redis.example.com:6379?password=secret&db=2&poll-timeout=45s&max-errors=5
```

## Comparison with HTTP Transport

### Similarities

1. **Session-based encryption**: Both use Age key exchange + ChaCha20Poly1305
2. **Long polling**: BLPOP timeout mimics HTTP GET with timeout
3. **Channel abstraction**: Both use Send/Recv channels
4. **Envelope protocol**: Same protobuf messages
5. **Error handling**: Similar retry logic and max error counting

### Differences

| Feature              | HTTP Transport                    | Redis Transport                |
|---------------------|-----------------------------------|--------------------------------|
| Protocol            | HTTP/HTTPS                        | Redis protocol                 |
| Session ID storage  | Cookies                           | Encrypted in initialization    |
| Polling mechanism   | HTTP GET with timeout             | BLPOP with timeout             |
| Message queueing    | Server-side in-memory channels    | Redis Lists (persistent)       |
| Reconnect recovery  | No message persistence            | Messages persist in Redis      |
| Network signature   | HTTP headers, TLS                 | Redis protocol (distinct)      |
| Authentication      | HTTP auth, cookies, OTP           | Redis password, key exchange   |
| Infrastructure      | Web server                        | Redis server                   |

## Advantages

1. **Message Persistence**: Messages persist in Redis if implant disconnects
2. **Simplified Protocol**: No HTTP headers, cookies, or TLS complexity
3. **Built-in Queueing**: Redis Lists provide natural FIFO queuing
4. **Performance**: Direct queue operations with minimal overhead
5. **Multiplexing**: Easy to extend for broadcast/multicast patterns
6. **Monitoring**: Redis CLI commands for session visibility
7. **Scalability**: Redis clustering support for high-scale deployments

## Disadvantages

1. **Network Signature**: Redis protocol is distinctive (consider Redis over TLS)
2. **Infrastructure**: Requires Redis server accessibility to implants
3. **Less Common**: HTTP blends into normal traffic better
4. **Detection**: Redis connections may be unusual in some environments
5. **Firewall Rules**: May require specific firewall configurations

## Next Steps for Production

### 1. Add TLS Support

The `TLSEnabled` field is defined but not implemented. Add:

```go
// In redisclient.go
if opts.TLSEnabled {
    redisOpts.TLSConfig = &tls.Config{
        InsecureSkipVerify: false, // Configure properly
        // Add cert verification
    }
}
```

### 2. Improve Session Addressing

Current implementation uses a simple response queue pattern. For production:
- Use unique nonces for init response addressing
- Implement request-response correlation IDs
- Add support for multiple concurrent initializations

### 3. Add to Build System

Update implant generation to support `IncludeRedis` build tag:
- Add to `server/generate/implant.go`
- Update build configuration options
- Add to implant config structure

### 4. Implement RPC Handler

Create server RPC handler in `server/rpc/rpc-redis.go`:

```go
func (rpc *Server) StartRedisListener(ctx context.Context, req *clientpb.RedisListenerReq) (*clientpb.ListenerJob, error) {
    server, err := c2.StartRedisListener(req)
    if err != nil {
        return nil, err
    }

    job := &core.Job{
        ID:          core.NextJobID(),
        Name:        "redis",
        Description: fmt.Sprintf("Redis listener on %s:%d", req.Host, req.Port),
        Protocol:    "redis",
        Port:        req.Port,
        JobCtrl:     make(chan bool),
    }

    go func() {
        <-job.JobCtrl
        server.Cleanup()
    }()

    core.Jobs.Add(job)

    return &clientpb.ListenerJob{
        JobID: job.ID,
    }, nil
}
```

### 5. Add CLI Commands

Add Sliver console commands in `client/command/`:

```go
// Start Redis listener
redis-listener --lhost 0.0.0.0 --lport 6379 --password secret

// Generate implant with Redis C2
generate --redis redis://192.168.1.100:6379?password=secret
```

### 6. Add Job Management

Update `server/c2/jobs.go` to track Redis listener jobs:
- Add to listener job types
- Implement stop/restart functionality
- Add to job status reporting

### 7. Security Enhancements

- **Rate limiting**: Prevent abuse of init queue
- **IP filtering**: Whitelist allowed implant IPs
- **Request signing**: Add HMAC to prevent replay attacks
- **Key rotation**: Support periodic key rotation
- **OTP integration**: Implement EnforceOTP support

### 8. Monitoring & Metrics

Add instrumentation:
- Session count metrics
- Message throughput
- Error rates
- Queue depth monitoring
- Latency tracking

### 9. Testing

Create test suite:
- Unit tests for client/server components
- Integration tests with real Redis
- Load testing for scalability
- Failure scenario testing (network drops, Redis crashes)

### 10. Documentation

- User guide for operators
- Configuration best practices
- Troubleshooting guide
- Security considerations
- Performance tuning guide

## Dependencies

Add to `go.mod`:

```go
require (
    github.com/go-redis/redis/v8 v8.11.5
)
```

Install dependency:

```bash
go get github.com/go-redis/redis/v8
```

## Testing the Implementation

### 1. Start Redis Server

```bash
# Using Docker
docker run -d --name redis -p 6379:6379 redis:latest

# With password
docker run -d --name redis -p 6379:6379 redis:latest --requirepass secret
```

### 2. Start Sliver Server (after RPC implementation)

```
sliver > redis-listener --lhost 0.0.0.0 --lport 6379
```

### 3. Generate Implant (after build integration)

```
sliver > generate --redis redis://192.168.1.100:6379
```

### 4. Monitor Redis (debugging)

```bash
# Watch all commands
redis-cli monitor

# Check queues
redis-cli KEYS "sliver:*"
redis-cli LLEN sliver:init

# Inspect session
redis-cli LLEN sliver:sessions:{sessionID}:upstream
redis-cli LLEN sliver:sessions:{sessionID}:downstream
redis-cli TTL sliver:sessions:{sessionID}:heartbeat
```

## Troubleshooting

### Implant can't connect

1. Check Redis is accessible: `redis-cli -h HOST -p PORT ping`
2. Verify password: `redis-cli -h HOST -p PORT -a PASSWORD ping`
3. Check firewall rules for Redis port
4. Verify network connectivity

### Session initialization fails

1. Check server logs for Age decryption errors
2. Verify both sides use same server public key
3. Check Redis queue: `redis-cli LLEN sliver:init`
4. Monitor Redis: `redis-cli MONITOR`

### Messages not delivered

1. Check queue lengths: `LLEN sliver:sessions:{sessionID}:upstream`
2. Verify encryption/decryption works
3. Check for errors in server logs
4. Verify session is still active

### High latency

1. Reduce poll-timeout for faster response
2. Check network latency to Redis
3. Monitor Redis performance
4. Consider Redis optimization (persistence settings)

## Security Considerations

### Network Security

1. **Use TLS**: Always enable TLS in production
2. **Redis AUTH**: Always use strong passwords
3. **Network isolation**: Run Redis in isolated network segment
4. **Firewall rules**: Restrict Redis access to known IPs

### Operational Security

1. **Key management**: Protect Age private keys
2. **Session monitoring**: Monitor for unusual sessions
3. **Rate limiting**: Prevent DoS on init queue
4. **Audit logging**: Log all session creation/termination

### Detection Avoidance

Redis protocol is distinct and may be detected:
1. **Use Redis over TLS** to encrypt protocol
2. **Custom ports**: Use non-standard ports
3. **Traffic shaping**: Add jitter to timing patterns
4. **Consider alternatives**: HTTP may blend better

## Conclusion

This Redis transport implementation provides a robust, queue-based alternative to HTTP C2 communication. It leverages Redis's built-in message persistence and queuing capabilities while maintaining compatibility with Sliver's existing transport architecture.

The implementation is production-ready from an architecture perspective but requires additional integration work for:
- Build system integration
- RPC handler implementation
- CLI command support
- TLS configuration
- Comprehensive testing

Future enhancements could include:
- Redis Pub/Sub for broadcast commands
- Redis Streams for better message tracking
- Multi-server Redis clustering
- Advanced queue prioritization
