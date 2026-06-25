# Design Document: On-Chain Event Correlation Service

## Overview

The On-Chain Event Correlation Service extends the existing `OnchainModule` to periodically fetch Soroban smart contract events from the Stellar RPC, map them to internal `AidPackage` and `Claim` records, and persist the correlated data to a dedicated `OnChainEvent` table. The service also exposes an on-demand HTTP endpoint and surfaces correlated events on the existing claim and package read endpoints.

### Goals
- Auditable, idempotent persistence of every on-chain AidEscrow event
- Automatic correlation linking `txHash`/`ledger`/`topic` to internal `packageId`/`claimId`
- Scheduled background ingestion (every 5 minutes) with cursor-based resumption
- On-demand trigger via `POST /v1/onchain/events/correlate`
- Zero breaking changes to existing endpoints — `onChainEvents` is additive

### Non-Goals
- Real-time WebSocket event streaming
- Cross-chain support beyond Stellar/Soroban
- Backfill of events predating the first deployment

---

## Architecture

### Component Diagram

```mermaid
graph TD
  subgraph OnchainModule
    ECS[EventCorrelationService]
    EM[EventMapper]
    ECC[EventCorrelationController]
    SA[SorobanAdapter]
  end

  subgraph ExistingServices
    CS[ClaimsService]
    AES[AidEscrowService]
    PS[PrismaService]
  end

  CRON((Cron @5min)) --> ECS
  HTTP[POST /v1/onchain/events/correlate] --> ECC
  ECC --> ECS
  ECS --> SA
  SA -->|getEvents RPC| RPC[(Soroban RPC)]
  ECS --> EM
  EM -->|DB lookup packageId/claimId| PS
  ECS -->|upsert OnChainEvent| PS
  ECS -->|upsert CorrelationCursor| PS
  CS -->|include onChainEvents| PS
  AES -->|include onChainEvents| PS

  subgraph DB[(SQLite via Prisma)]
    OC[OnChainEvent]
    CC[CorrelationCursor]
  end
```


### Data Flow

1. **Trigger** — Cron fires every 5 min OR `POST /v1/onchain/events/correlate` is called
2. **Read cursor** — `EventCorrelationService` reads `CorrelationCursor` row (key=`"default"`) to get `lastLedger`
3. **Fetch events** — calls `SorobanRpc.Server.getEvents({ startLedger, ..., filters: [contractId] })`
4. **Map** — each raw event is passed to `EventMapper.map(rawEvent)` → `OnChainEventPayload`
5. **DB lookups** — inside `EventMapper`, optional lookups against `AidPackage` / `Claim` tables resolve foreign keys
6. **Upsert** — `EventCorrelationService` calls `prisma.onChainEvent.upsert` keyed on `(txHash, topic)`
7. **Update cursor** — after all events processed, cursor is updated to `max(ledger)`
8. **Return result** — `{ fetched, correlated, inserted, updated }` returned to caller

---

## Components and Interfaces

### 1. EventMapper

A pure, side-effect-free class. It receives a raw `SorobanRpc.Api.EventResponse` event and returns an `OnChainEventPayload`. DB lookups are injected via `PrismaService`.

**File:** `app/backend/src/onchain/event-correlation/event-mapper.ts`

```typescript
// Input type — subset of SorobanRpc.Api.EventResponse
export interface RawSorobanEvent {
  id: string;            // Soroban event ID (used to derive txHash)
  type: string;          // "contract"
  ledger: number;
  txHash: string;
  contractId: string;
  topic: xdr.ScVal[];    // Array of ScVal, first entry typically encodes the event name
  value: xdr.ScVal;      // The event body
}

// Output type (matches OnChainEvent Prisma model fields)
export interface OnChainEventPayload {
  txHash: string;
  ledger: number;
  topic: string;         // Decoded string label, e.g. "package_created"
  contractId: string;
  packageId: string | null;
  claimId: string | null;
  rawPayload: object;    // scValToNative(value) serialised as JSON-safe object
  correlatedAt: Date;
}
```


**Extraction logic:**

- `txHash` — taken directly from `event.txHash`
- `ledger` — taken from `event.ledger`
- `topic` — `scValToNative(event.topic[0])` cast to string; falls back to `"unknown"` if topic array is empty or decoding fails
- `contractId` — taken from `event.contractId`
- `package_id` — scan `event.topic[1..n]` and `scValToNative(event.value)` for a key named `"package_id"` or `"id"` when topic is `"package_created"` / `"package_claimed"` / `"package_disbursed"`; value is cast to string
- `claim_id` — scan for a key named `"claim_id"`; value is a CUID string
- `rawPayload` — `scValToNative(event.value)` — the full decoded body stored as JSON
- If `package_id` found → `prisma.aidPackage.findFirst({ where: { id: packageId } })`; set FK or null if not found
- If `claim_id` found → `prisma.claim.findUnique({ where: { id: claimId } })`; set FK or null if not found
- All exceptions within `map()` are caught and result in a payload with both FKs null (logged as warning, not re-thrown)

### 2. EventCorrelationService

**File:** `app/backend/src/onchain/event-correlation/event-correlation.service.ts`

```typescript
export interface CorrelationResult {
  fetched: number;
  correlated: number;
  inserted: number;
  updated: number;
  startLedger: number;
  endLedger: number;
  error?: string;
}
```

Key responsibilities:

- `correlate(startLedger?: number, endLedger?: number): Promise<CorrelationResult>` — core public method
- `@Cron(cronExpression)` decorator on `scheduledCorrelate()` — reads cursor, calls `correlate()`, updates cursor
- `getCursor(): Promise<number>` — reads `CorrelationCursor` where `key = "default"`, returns `lastLedger` (default: `latestLedger - 1` on first run)
- `setCursor(ledger: number): Promise<void>` — upserts `CorrelationCursor` by `key = "default"`
- Concurrency guard — `private isRunning = false` flag; `scheduledCorrelate()` skips and logs if already `true`
- Fetches events via `SorobanRpc.Server.getEvents` directly (not through the OnchainAdapter, since `getEvents` is an RPC-level call not currently in the adapter interface)


### 3. EventCorrelationController

**File:** `app/backend/src/onchain/event-correlation/event-correlation.controller.ts`

```typescript
@Controller('v1/onchain/events')
export class EventCorrelationController {
  @Post('correlate')
  async correlate(
    @Query('startLedger') startLedger?: string,
    @Query('endLedger') endLedger?: string,
  ): Promise<CorrelationResult>
}
```

Validation rules:
- If `startLedger` is provided: parse to integer; if `NaN` or `<= 0` → throw `BadRequestException`
- If `endLedger` is provided: parse to integer; if `< startLedger` → throw `BadRequestException`
- If `EventCorrelationService.correlate()` throws an RPC-connectivity error → map to `HttpException(502)`

### 4. Scheduler Integration

`EventCorrelationService` injects the cron expression from `ConfigService`:

```typescript
private readonly cronExpression =
  this.configService.get<string>('CORRELATION_CRON_EXPRESSION') ?? '*/5 * * * *';

@Cron(/* dynamic via factory approach using SchedulerRegistry */ )
async scheduledCorrelate(): Promise<void> { ... }
```

Because NestJS `@Cron` requires a compile-time literal, the recommended approach is to use `SchedulerRegistry` to add a dynamic cron job in `onModuleInit()`, or use a `@Cron(CronExpression.EVERY_5_MINUTES)` as default and override with `SchedulerRegistry` when `CORRELATION_CRON_EXPRESSION` is set. The simplest compliant implementation:

```typescript
@Cron('*/5 * * * *', { name: 'event-correlation' })
async scheduledCorrelate(): Promise<void> {
  // On startup, if CORRELATION_CRON_EXPRESSION differs from default,
  // the SchedulerRegistry replaces the job's cron time.
}
```

---

## Data Models

### Prisma Schema Additions

```prisma
/// Stores each correlated on-chain event from the AidEscrow Soroban contract
model OnChainEvent {
  id            String   @id @default(cuid())
  txHash        String
  ledger        Int
  topic         String
  contractId    String
  packageId     String?
  claimId       String?
  rawPayload    Json
  correlatedAt  DateTime @default(now())

  @@unique([txHash, topic])
  @@index([packageId])
  @@index([claimId])
  @@index([ledger])
  @@index([correlatedAt])
}

/// Persists the last processed ledger number for cursor-based polling
model CorrelationCursor {
  key         String   @id   // Always "default" for the single global cursor
  lastLedger  Int
  updatedAt   DateTime @updatedAt
}
```


**Design decisions:**

- `rawPayload` uses `Json` type (SQLite stores as JSON-encoded text). This is consistent with existing `metadata Json?` fields in the schema.
- `@@unique([txHash, topic])` enables idempotent upsert keyed on `(txHash, topic)`.
- Both `packageId` and `claimId` are nullable strings (not relations) to avoid FK constraint failures when correlating events for packages/claims that don't yet exist in the DB.
- `CorrelationCursor` uses a string `key` PK (`"default"`) to allow future multi-contract cursors without schema changes.

---

## Updates to Existing Endpoints

### claims.service.ts — `findOne`

```typescript
async findOne(id: string) {
  const claimResult = await this.prisma.claim.findUnique({
    where: { id },
    include: {
      campaign: true,
    },
  });
  // ... existing null/deletedAt check ...

  // NEW: fetch associated on-chain events
  const onChainEvents = await this.prisma.onChainEvent.findMany({
    where: { claimId: id },
    select: { txHash: true, ledger: true, topic: true, correlatedAt: true, contractId: true, rawPayload: true },
    orderBy: { ledger: 'asc' },
  });

  return {
    ...claim,
    recipientRef: this.encryptionService.decrypt(claim.recipientRef),
    onChainEvents,   // always present; empty array when none exist
  };
}
```

### aid-escrow.controller.ts — `getAidPackage`

The controller calls `aidEscrowService.getAidPackage()` which fetches the package from the Soroban RPC. Since this is an on-chain read (not from Prisma), the `onChainEvents` are fetched separately in the controller using an injected `PrismaService`:

```typescript
@Get('packages/:id')
async getAidPackage(@Param('id') packageId: string): Promise<any> {
  try {
    const [onchainPkg, onChainEvents] = await Promise.all([
      this.aidEscrowService.getAidPackage({ packageId }),
      this.prisma.onChainEvent.findMany({
        where: { packageId },
        select: { txHash: true, ledger: true, topic: true, correlatedAt: true },
        orderBy: { ledger: 'asc' },
      }),
    ]);
    return { ...onchainPkg, onChainEvents };
  } catch (error) {
    this.logger.error('Failed to get aid package:', error);
    this.errorMapper.throwMappedError(error);
  }
}
```

`PrismaService` is added to `AidEscrowModule`'s providers and injected into `AidEscrowController`. `PrismaModule` is already globally available in the project.

---

## Module Wiring

### New files under `app/backend/src/onchain/event-correlation/`

```
event-correlation/
  event-correlation.service.ts
  event-correlation.controller.ts
  event-mapper.ts
  dto/
    correlation-result.dto.ts
```

### OnchainModule additions

```typescript
// onchain.module.ts (additions only)
import { ScheduleModule } from '@nestjs/schedule';
import { EventCorrelationService } from './event-correlation/event-correlation.service';
import { EventCorrelationController } from './event-correlation/event-correlation.controller';
import { EventMapper } from './event-correlation/event-mapper';

@Module({
  imports: [
    ConfigModule,
    ScheduleModule.forRoot(),   // if not already registered at app root
    // ... existing BullModule, JobsModule, LoggerModule, MetricsModule
  ],
  controllers: [
    LedgerAdminController,
    EventCorrelationController,   // NEW
  ],
  providers: [
    // ... existing providers
    EventCorrelationService,   // NEW
    EventMapper,               // NEW
  ],
  exports: [
    ONCHAIN_ADAPTER_TOKEN,
    OnchainService,
    LedgerBackfillService,
    LedgerReconciliationService,
    EventCorrelationService,   // NEW — exported for potential cross-module use
  ],
})
export class OnchainModule {}
```

> **Note:** `ScheduleModule.forRoot()` should be registered once at the app root module level. If it is already present in `AppModule`, it should not be re-added here. The project uses `@nestjs/schedule` (confirmed via `ClaimsService` which already uses `@Cron`).


---

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

**Property reflection before finalizing:**

After reviewing all testable criteria from the prework:
- Properties 3.1 (mapper extracts fields) and 9.4 (round-trip serialization) are distinct and complementary — 3.1 tests extraction logic, 9.4 tests serialization fidelity.
- Properties 7.1 and 8.1 (response always includes `onChainEvents`) are analogous but target different endpoints — kept separate.
- Properties 7.3 and 8.3 (response event shape) can be **combined** into one property since both verify the same `{ txHash, ledger, topic, correlatedAt }` shape invariant.
- Properties 4.1 (upsert idempotency) and 4.3 (batch count invariant) are distinct.
- Properties 5.2 and 5.3 (input validation) are distinct and kept separate.

The finalized, non-redundant property set follows.

---

### Property 1: Mapper always extracts required fields from valid events

*For any* well-formed Soroban event that has non-null `txHash`, `ledger`, `contractId`, and at least one entry in `topic`, the `EventMapper.map()` output SHALL have `txHash`, `ledger`, `topic`, and `contractId` set to non-null, non-empty values matching the input.

**Validates: Requirements 3.1, 9.1**

---

### Property 2: Mapper produces null foreign keys for events with no package_id or claim_id

*For any* Soroban event whose `topic` array and decoded `value` contain no field named `"package_id"` or `"claim_id"`, the `EventMapper.map()` output SHALL have `packageId = null` and `claimId = null`.

**Validates: Requirements 3.5, 9.2**

---

### Property 3: Mapper never throws on malformed input

*For any* object passed to `EventMapper.map()` — including objects with missing fields, wrong types, null values, or empty arrays — the method SHALL return an `OnChainEventPayload` without throwing an unhandled exception.

**Validates: Requirements 9.3**

---

### Property 4: Mapper output round-trips through JSON without data loss

*For any* valid `OnChainEventPayload` produced by `EventMapper.map()`, serializing the payload with `JSON.stringify` and then deserializing with `JSON.parse` SHALL produce an object that is structurally equivalent to the original (same keys, same primitive values, same null/non-null fields).

**Validates: Requirements 1.1, 9.4**

---

### Property 5: Upsert is idempotent — same (txHash, topic) produces exactly one DB row

*For any* valid `OnChainEventPayload`, calling the Correlator's persist operation any number of times with the same `(txHash, topic)` pair SHALL result in exactly one `OnChainEvent` row in the database.

**Validates: Requirements 4.1**

---

### Property 6: Batch inserted + updated counts equal total events processed

*For any* batch of N events where K events have `(txHash, topic)` pairs already present in the database, the `CorrelationResult` SHALL satisfy: `inserted + updated = N`, `updated = K`, and `inserted = N - K`.

**Validates: Requirements 4.3**

---

### Property 7: Correlation endpoint rejects invalid startLedger values

*For any* call to `POST /v1/onchain/events/correlate` where `startLedger` is provided as a non-positive integer (zero, negative, non-numeric string, or decimal), the endpoint SHALL return HTTP 400.

**Validates: Requirements 5.2**

---

### Property 8: Correlation endpoint rejects endLedger less than startLedger

*For any* pair `(startLedger, endLedger)` where both are positive integers and `endLedger < startLedger`, the endpoint SHALL return HTTP 400.

**Validates: Requirements 5.3**

---

### Property 9: Cursor advances to max processed ledger after any correlation run

*For any* correlation run that successfully processes at least one event, the `CorrelationCursor` value after the run SHALL equal the maximum `ledger` value among all events processed in that run.

**Validates: Requirements 6.3**

---

### Property 10: Claim read response always includes onChainEvents array

*For any* valid `GET /v1/claims/:id` request on a non-deleted claim, the response SHALL include an `onChainEvents` field that is an array (empty array when no correlated events exist, never absent or null).

**Validates: Requirements 7.1, 7.2**

---

### Property 11: Package read response always includes onChainEvents array

*For any* valid `GET /v1/aid/packages/:id` request, the response SHALL include an `onChainEvents` field that is an array (empty array when no correlated events exist, never absent or null).

**Validates: Requirements 8.1, 8.2**

---

### Property 12: OnChainEvent items in responses include required fields

*For any* `OnChainEvent` item returned in either the claim or package read response, the item SHALL include `txHash` (non-empty string), `ledger` (positive integer), `topic` (non-empty string), and `correlatedAt` (ISO date string).

**Validates: Requirements 7.3, 8.3**


---

## Error Handling

| Scenario | Handling |
|---|---|
| Soroban RPC unreachable (fetch phase) | Log structured error with ledger range; return `CorrelationResult` with `error` field set; do **not** update cursor; controller maps to HTTP 502 |
| RPC returns JSON-RPC error object | Same as above — caught, logged, returned as structured result |
| `EventMapper.map()` throws | Caught per-event; event skipped; warning logged with raw event ID; correlation continues for remaining events |
| DB upsert fails (transient) | Caught per-event; logged; counted in a separate `failed` counter (not exposed in API but in logs) |
| Concurrent scheduler runs | `isRunning` flag checked in `scheduledCorrelate()`; second trigger logs `WARN: correlation already in progress` and returns immediately |
| `SOROBAN_RPC_URL` not set | `EventCorrelationService` constructor throws `Error` during module initialization, preventing app startup |
| Invalid ledger range on HTTP endpoint | `BadRequestException` with descriptive message returned before any RPC call is made |

---

## Testing Strategy

### Unit Tests — EventMapper (`app/backend/src/onchain/__tests__/event-mapper.spec.ts`)

Property-based tests use **fast-check** (already commonly available in NestJS/TypeScript projects via `npm install --save-dev fast-check`).

```typescript
import * as fc from 'fast-check';
```

Each property test is configured for a minimum of **100 iterations**.

**Test file structure:**

```
app/backend/src/onchain/__tests__/
  event-mapper.spec.ts     ← property + unit tests for EventMapper
  event-correlation.service.spec.ts  ← unit tests for service (mocked Prisma + RPC)
```

**Fixtures used in tests:**

```typescript
// Well-formed Soroban event fixture
const wellFormedEvent: RawSorobanEvent = {
  id: 'event-001',
  type: 'contract',
  ledger: 1234,
  txHash: 'ABCD1234ABCD1234ABCD1234ABCD1234ABCD1234ABCD1234ABCD1234ABCD1234',
  contractId: 'CABC123...',
  topic: [
    nativeToScVal('package_created', { type: 'symbol' }),
    nativeToScVal({ package_id: '42' }, { type: 'map' }),
  ],
  value: nativeToScVal({ amount: '1000', recipient: 'GABC...' }),
};
```

**Property test tagging format:**

```typescript
// Feature: on-chain-event-correlation, Property 1: Mapper always extracts required fields from valid events
it('Property 1: extracts required fields from any valid event', () => {
  fc.assert(
    fc.property(validSorobanEventArb, event => {
      const result = mapper.map(event);
      expect(result.txHash).toBeTruthy();
      expect(result.ledger).toBeGreaterThan(0);
      expect(result.topic).toBeTruthy();
      expect(result.contractId).toBeTruthy();
    }),
    { numRuns: 100 },
  );
});
```

**Arbitraries (fast-check generators):**

```typescript
const validSorobanEventArb = fc.record({
  id: fc.string({ minLength: 1 }),
  type: fc.constant('contract'),
  ledger: fc.integer({ min: 1, max: 1_000_000 }),
  txHash: fc.hexaString({ minLength: 64, maxLength: 64 }),
  contractId: fc.string({ minLength: 56, maxLength: 56 }),
  topic: fc.array(fc.anything(), { minLength: 1 }),
  value: fc.anything(),
});

const malformedEventArb = fc.oneof(
  fc.constant(null),
  fc.constant({}),
  fc.record({ txHash: fc.constant(null), ledger: fc.string() }),
  fc.anything(),  // completely random object
);
```


**Tests to implement:**

| Test ID | Type | Description |
|---|---|---|
| P1 | Property (100 runs) | Mapper extracts txHash/ledger/topic/contractId from any valid event |
| P2 | Property (100 runs) | Mapper returns packageId=null, claimId=null for events without package_id/claim_id |
| P3 | Property (100 runs) | Mapper never throws on any malformed/arbitrary input |
| P4 | Property (100 runs) | Mapper output round-trips through JSON.stringify/parse |
| E1 | Example | Mapper extracts package_id from a well-formed package_created event |
| E2 | Example | Mapper extracts claim_id from a well-formed claim event |
| E3 | Example | Mapper returns null packageId when DB lookup returns null |
| E4 | Example | Correlator returns fetched=0, correlated=0 for empty RPC response |
| E5 | Example | Upsert on duplicate (txHash, topic) updates rawPayload, not inserts new row |
| E6 | Example | Controller returns HTTP 400 when startLedger=0 |
| E7 | Example | Controller returns HTTP 400 when endLedger < startLedger |
| E8 | Example | Controller returns HTTP 502 when RPC throws network error |
| E9 | Example | GET /claims/:id response includes empty onChainEvents array |
| E10 | Example | GET /aid-escrow/packages/:id response includes populated onChainEvents |

### Integration Tests

- Verify `CorrelationCursor` is created on first run (no pre-existing cursor row)
- Verify cursor advances after successful run
- Verify idempotent upsert across two successive runs with the same ledger range

### Running Tests

```bash
# From app/backend/
npx jest --testPathPattern="event-mapper|event-correlation" --run
```

No additional configuration beyond the existing `jest.config.js` is required.

---

## Configuration Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `STELLAR_RPC_URL` | Yes | — | Soroban RPC URL (existing key, reused as `SOROBAN_RPC_URL`) |
| `AID_ESCROW_CONTRACT_ID` | Yes | — | Contract ID to filter events |
| `CORRELATION_CRON_EXPRESSION` | No | `*/5 * * * *` | Cron schedule for background correlation |
| `CORRELATION_PAGE_SIZE` | No | `200` | Max events per RPC `getEvents` call |

> The `SOROBAN_RPC_URL` config key referenced in the requirements maps to the existing `STELLAR_RPC_URL` environment variable. `EventCorrelationService` reads it via `configService.get<string>('STELLAR_RPC_URL')`. No new env var needs to be added to deployment manifests.
