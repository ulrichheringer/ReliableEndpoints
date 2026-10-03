# ReliableEndpoints

Small declarative resilience extensions for ASP.NET Core on .NET 8. Idempotency keys, response replay, per-resource locks, cooperative endpoint timeouts and tracing, with a real PostgreSQL backend and process-local providers for development. Explicit operation retries avoid executing the entire HTTP pipeline twice.

## Install from source

The initial release is distributed as source on GitHub; no NuGet publication is implied.

```sh
dotnet add YourApp.csproj reference path/to/ReliableEndpoints/src/ReliableEndpoints/ReliableEndpoints.csproj
# Or create a package and install it from a local feed:
dotnet pack src/ReliableEndpoints -c Release -o artifacts
dotnet add YourApp.csproj package ReliableEndpoints --version 0.1.0 --source ./artifacts
```

## Quick start

```csharp
using ReliableEndpoints;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddReliableEndpoints(); // process-local defaults
var app = builder.Build();
app.UseRouting();
app.UseAuthentication(); // when configured
app.UseAuthorization();
app.UseReliableEndpoints(); // after routing and authorization, before handlers
app.MapPost("/orders", () => Results.Created("/orders/42", new { id = 42 }))
    .WithReliability(new EndpointReliabilityOptions
    {
        KeyScope = ctx => ctx.User.FindFirst("sub")!.Value,
        Retention = TimeSpan.FromHours(24),
        Timeout = TimeSpan.FromSeconds(10),
        LockKey = ctx => "orders:" + ctx.User.FindFirst("sub")!.Value
    }).RequireAuthorization();
app.Run();
```

Send one `Idempotency-Key` header, up to 256 characters. Scope is required: use a trusted authenticated user or tenant identity. Do not use an unvalidated client header as a production security boundary. The demo uses a fixed identity for local experimentation only:

```sh
dotnet run --project examples/Demo --urls http://localhost:5080
curl -X POST http://localhost:5080/orders -H 'Idempotency-Key: order-123'
curl -X POST http://localhost:5080/orders -H 'Idempotency-Key: order-123'
```

## PostgreSQL across multiple instances

Register the PostgreSQL implementations before the default registrations. All instances must use the same database and schema. Npgsql is the only runtime package dependency; it owns connection pooling.

```csharp
using Npgsql;
using ReliableEndpoints;

builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(
    builder.Configuration.GetConnectionString("Reliability")!));
builder.Services.AddSingleton<PostgresIdempotencyStore>();
builder.Services.AddSingleton<IIdempotencyStore>(sp =>
    sp.GetRequiredService<PostgresIdempotencyStore>());
builder.Services.AddSingleton<IDistributedLockProvider, PostgresLockProvider>();
builder.Services.AddReliableEndpoints();
// After building the app, before accepting requests:
await app.Services.GetRequiredService<PostgresIdempotencyStore>().EnsureSchemaAsync();
```

The schema method explicitly creates `reliable_endpoint_responses` in the connection's search path. In managed deployments run schema creation as a migration with suitable permissions. Periodically call `PurgeExpiredAsync` to remove expired rows. Response bodies are persisted as JSON/base64; protect the database as you would the original API data.

Session advisory locks keep one dedicated connection per active idempotent request and another when `LockKey` is enabled. Size the connection pool accordingly; contention returns immediately rather than waiting for a lock. Use session connections directly to PostgreSQL; transaction-mode PgBouncer is unsupported. Locks have no timer that could expire while a handler still runs. PostgreSQL releases them when the connection closes. All resource lock keys share a namespace: include the tenant and resource in `LockKey` when isolation is required.

## Behavior and guarantees

| Situation | Result |
| --- | --- |
| Missing, empty, duplicate or too-long key | 400 |
| Request exceeds `MaximumBodyBytes` (default 1 MiB) | 413 before handler |
| Same scoped key executing | 409 |
| Same key with changed request body, content type or query | 409 once a response is retained |
| Same scoped key completed | Stored status/body, Content-Type and Location; `Idempotency-Replayed: true` |
| Resource lock busy | 409, idempotency lease released |
| Handler returns 5xx or throws | Response not retained; lease released |
| Timeout | Cooperative cancellation; 504 after handler exits |

The key includes scope, method and path; the fingerprint includes raw query, content type and exact body bytes. Semantically equivalent JSON with different whitespace counts as a different payload. Headers beyond Content-Type are not included in the fingerprint; move any business-relevant variation into the scope or request payload. All status codes below 500 are retained, including deterministic 4xx errors. Cookies and authorization headers are never replayed. A completed response remains until retention expires. Process-local retained entries are removed lazily when the key is accessed again; use PostgreSQL for durable or high-cardinality workloads.

Handlers must honor `HttpContext.RequestAborted`. Timeout does not abandon a running task or release its lock while it is still executing. Handlers ignoring cancellation can exceed the timeout. This library buffers finite responses; streaming, explicit `StartAsync`, server-sent events, upgrades and early header flushes are unsupported. Responses larger than the limit throw before replay storage, so configure appropriate bounds and exception handling. Content-Length is produced from the actual output on replay rather than blindly copied.

Idempotency storage and business effects are not one transaction. A process crash or database connection loss after a business effect but before completion can allow execution again. Use database unique constraints, business-level idempotency or an outbox for atomic guarantees. Advisory locks are advisory and cannot fence an external service after network failure. The PostgreSQL provider preserves successful responses across restarts, but does not claim exactly-once side effects.

## Safe retries

```csharp
var data = await Retry.ExecuteAsync(
    ct => client.GetStringAsync("https://example.com/status", ct),
    new RetryOptions
    {
        MaximumAttempts = 3, // includes the first attempt
        Delay = TimeSpan.FromMilliseconds(100),
        ShouldRetry = ex => ex is HttpRequestException
    }, httpContext.RequestAborted);
```

Retry only operations whose effects are safe to repeat. The delay is fixed and cancellable. Cancellation is never retried. The exception from the final attempt propagates. The HTTP middleware itself does not retry handlers.

## Tracing and custom backends

Subscribe to the `ReliableEndpoints` ActivitySource (for example with OpenTelemetry `.AddSource("ReliableEndpoints")`). Each protected request emits `endpoint.execute` with `idempotency.status`; cooperative timeouts mark the span as an error. Keys, payloads and identities are not exported.

Implement `IIdempotencyStore` with atomic acquisition, token-fenced completion and owned release, and `IDistributedLockProvider` with immediate contention results. Backend errors must fail closed. The bundled local providers cannot coordinate separate processes.

## Development

```sh
dotnet restore
dotnet format --verify-no-changes --no-restore
dotnet build -c Release --no-restore
dotnet test -c Release
# To include real PostgreSQL integration tests:
# set RELIABLE_ENDPOINTS_POSTGRES to a test database connection string, then:
dotnet test -c Release
dotnet pack src/ReliableEndpoints -c Release --no-build
```

CI runs the same checks with PostgreSQL 16. Compiler and .NET analyzers treat warnings as errors. Tests cover replay, concurrency, fingerprint conflicts, tenant isolation, timeout cleanup, server errors, payload limits, token ownership, expiration, retries and locks. Without the environment variable, the PostgreSQL integration test is explicitly skipped; CI always sets it and runs the real backend. The development database should be disposable.

MIT licensed. Contributions should include tests for changed behavior and pass the checks above.
