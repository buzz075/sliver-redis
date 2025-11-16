# Redis RPC Handler Implementation Summary

This document summarizes the RPC handler implementation for the Redis C2 listener.

## Files Modified/Created

### 1. **[client/constants/constants.go](client/constants/constants.go#L190)**
Added Redis constant for listener type identification:
```go
RedisStr = "redis"
```

### 2. **[server/c2/jobs.go](server/c2/jobs.go#L172-L206)**
Created `StartRedisListenerJob()` function:
- Starts Redis listener via `StartRedisListener()`
- Creates job with unique ID and description
- Sets up cleanup handler for graceful shutdown
- Publishes job stopped events

### 3. **[server/rpc/rpc-jobs.go](server/rpc/rpc-jobs.go)**
#### Added constant (line 42):
```go
defaultRedisPort = 6379
```

#### Created `StartRedisListener()` RPC handler (lines 281-311):
- Validates port (0-65535)
- Sets default port to 6379 if not specified
- Checks if port is already in use
- Starts Redis listener job
- Saves listener configuration to database
- Returns job information

#### Updated `PortInUse()` function (line 335-336):
Added Redis case to port conflict detection:
```go
case "redis":
    port = listener.RedisConf.Port
```

### 4. **[protobuf/clientpb/client.proto](protobuf/clientpb/client.proto)**

#### Added RedisListenerReq message (lines 407-414):
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

#### Updated ListenerJob message (line 363):
```protobuf
message ListenerJob {
  string ID = 1;
  string Type = 2;
  uint32 JobID = 3;
  MTLSListenerReq MTLSConf = 4;
  WGListenerReq WGConf = 5;
  DNSListenerReq DNSConf = 6;
  HTTPListenerReq HTTPConf = 7;
  MultiplayerListenerReq MultiConf = 8;
  StagerListenerReq TCPConf = 9;
  RedisListenerReq RedisConf = 10;  // NEW
}
```

### 5. **[protobuf/rpcpb/services.proto](protobuf/rpcpb/services.proto#L41)**
Added RPC service definition:
```protobuf
rpc StartRedisListener(clientpb.RedisListenerReq) returns (clientpb.ListenerJob);
```

### 6. **Dependencies**
Added to go.mod and vendored:
```
github.com/go-redis/redis/v8 v8.11.5
```

## RPC Handler Flow

### Start Redis Listener Request
```
Client Request
    ↓
StartRedisListener(ctx, RedisListenerReq)
    ↓
Validate port (0-65535)
    ↓
Set default port (6379) if not specified
    ↓
Check PortInUse()
    ↓
StartRedisListenerJob(req)
    ↓
    └─> StartRedisListener(req) [in c2/redis.go]
        └─> Create Redis client
        └─> Start session handlers
        └─> Return SliverRedisC2
    ↓
Create ListenerJob
    ↓
db.SaveC2Listener(listenerJob)
    ↓
Return JobID to client
```

### Job Lifecycle
```
Job Created
    ↓
core.Jobs.Add(job)
    ↓
Goroutine monitors job.JobCtrl
    ↓
When job.JobCtrl receives signal:
    ↓
    └─> server.Cleanup()
        └─> Cancel context
        └─> Close all sessions
        └─> Close Redis client
    ↓
    └─> core.Jobs.Remove(job)
    ↓
    └─> Publish JobStoppedEvent
```

## Next Steps

### 1. Compile Protobufs
```bash
make pb
```
This will generate:
- `protobuf/clientpb/client.pb.go` - Contains `RedisListenerReq` struct
- `protobuf/rpcpb/services.pb.go` - Contains RPC service interface
- `protobuf/rpcpb/services_grpc.pb.go` - Contains gRPC client/server stubs

### 2. Database Support (Optional)
The current implementation uses `db.SaveC2Listener()` and `db.ListenerByJobID()`. You may need to update the database schema/models to properly handle Redis listeners. Check:
- `server/db/listeners.go` or similar files
- Ensure `RedisConf` field is properly serialized/deserialized

### 3. Build System Integration
To enable Redis transport in generated implants, update:

**server/generate/implant.go** (or similar):
```go
type ImplantConfig struct {
    // ... existing fields ...
    IncludeRedis bool
}
```

Add build tag logic:
```go
if config.IncludeRedis {
    tags = append(tags, "redis")
}
```

### 4. CLI Commands
Create console commands for operators to start Redis listeners. Example location:
`client/command/jobs/redis.go`:

```go
func RedisListenerCmd(cmd *cobra.Command, con *console.SliverClient, args []string) {
    host, _ := cmd.Flags().GetString("lhost")
    port, _ := cmd.Flags().GetUint32("lport")
    password, _ := cmd.Flags().GetString("password")
    db, _ := cmd.Flags().GetUint32("db")

    req := &clientpb.RedisListenerReq{
        Host:     host,
        Port:     port,
        Password: password,
        DB:       db,
    }

    resp, err := con.Rpc.StartRedisListener(context.Background(), req)
    if err != nil {
        con.PrintErrorf("Failed to start Redis listener: %s\n", err)
        return
    }

    con.PrintInfof("Started Redis listener (Job ID: %d)\n", resp.JobID)
}
```

### 5. Generate Commands
Update implant generation commands to support Redis C2:

```go
// In client/command/generate/generate.go or similar
func addC2Flags(cmd *cobra.Command, config *ImplantConfig) {
    // ... existing flags ...
    cmd.Flags().StringSliceP("redis", "r", []string{},
        "Redis C2 URIs (e.g., redis://host:6379?password=secret)")
}
```

### 6. Testing

#### Unit Tests
Create `server/c2/redis_test.go`:
```go
func TestRedisListener(t *testing.T) {
    // Test session initialization
    // Test message encryption/decryption
    // Test session cleanup
}
```

#### Integration Tests
Create `server/rpc/rpc-redis_test.go`:
```go
func TestStartRedisListener(t *testing.T) {
    // Test RPC handler
    // Test port validation
    // Test job creation
}
```

#### Manual Testing
```bash
# Start Redis server
docker run -d --name redis -p 6379:6379 redis:latest

# In Sliver server console
sliver > redis-listener --lhost 0.0.0.0 --lport 6379

# Generate implant
sliver > generate --redis redis://192.168.1.100:6379
```

### 7. Documentation Updates
- User guide for Redis listener
- Security considerations
- Performance tuning
- Troubleshooting guide

## Configuration Examples

### Basic Redis Listener
```go
&clientpb.RedisListenerReq{
    Host: "0.0.0.0",
    Port: 6379,
}
```

### Authenticated Redis
```go
&clientpb.RedisListenerReq{
    Host:     "0.0.0.0",
    Port:     6379,
    Password: "strongpassword",
    DB:       1,
}
```

### Redis with TLS (Future)
```go
&clientpb.RedisListenerReq{
    Host:       "0.0.0.0",
    Port:       6380,
    Password:   "strongpassword",
    TLSEnabled: true,
}
```

## Architecture Summary

The RPC implementation follows Sliver's existing listener patterns:

1. **RPC Handler** (`server/rpc/rpc-jobs.go`)
   - Validates input
   - Checks port availability
   - Delegates to job creator

2. **Job Creator** (`server/c2/jobs.go`)
   - Creates core.Job
   - Sets up cleanup handlers
   - Manages lifecycle

3. **Listener** (`server/c2/redis.go`)
   - Implements actual Redis C2
   - Manages sessions
   - Handles encryption

4. **Protocol** (Protobuf definitions)
   - Request/response structures
   - RPC service definitions
   - Job configurations

This architecture ensures consistency with existing listeners (HTTP, DNS, mTLS, WG) while providing Redis-specific functionality.

## Debugging

### Check if RPC is registered
After compiling protobufs, verify the RPC method exists:
```bash
grep -r "StartRedisListener" protobuf/rpcpb/*.pb.go
```

### Verify database support
```bash
grep -r "RedisConf" server/db/
```

### Test Redis connection
```bash
redis-cli -h localhost -p 6379 ping
```

### Monitor Redis queues
```bash
redis-cli MONITOR
redis-cli KEYS "sliver:*"
```

## Summary

The RPC handler implementation is complete and follows Sliver's architecture patterns. The main components are:

✅ Constants defined
✅ Job creator implemented
✅ RPC handler created
✅ Port conflict detection updated
✅ Protobuf messages defined
✅ RPC service registered
✅ Dependencies added and vendored

**Status**: Core RPC implementation complete. Ready for protobuf compilation and further integration (CLI, build system, database).
