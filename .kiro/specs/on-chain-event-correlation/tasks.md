# Implementation Plan: On-Chain Event Correlation Service

## Overview

Implement the `EventCorrelationModule` in NestJS that fetches Soroban smart contract events, maps them to internal `AidPackage`/`Claim` records, persists them idempotently, and surfaces them on the existing claim and package read endpoints. The implementation follows the design's component breakdown: Prisma schema additions first, then the pure `EventMapper`, then the `EventCorrelationService` with its scheduler, then the controller, then the read-endpoint updates, and finally wiring everything into the module.

---

## Tasks

- [ ] 1. Add Prisma models and generate migration
  - [ ] 1.1 Add `OnChainEvent` and `CorrelationCursor` models to `prisma/schema.prisma`
    - Add the `OnChainEvent` model with fields: `id`, `txHash`, `ledger`, `topic`, `contractId`, `packageId` (nullable), `claimId` (nullable), `rawPayload` (Json), `correlatedAt`
    - Add `@@unique([txHash, topic])` constraint for idempotent upsert
    - Add `@@index` directives on `packageId`, `claimId`, `ledger`, `correlatedAt`
    - Add the `CorrelationCursor` model with `key` (String `@id`), `lastLedger` (Int), `updatedAt` (DateTime `@updatedAt`)
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [ ] 1.2 Run Prisma migration to apply new models to the database
    - Execute `npx prisma migrate dev --name add-on-chain-event-correlation` inside `app/backend/`
    - Verify the generated migration SQL contains the two new tables and the unique index
    - _Requirements: 1.1, 1.2, 1.3_

- [ ] 2. Create module skeleton and DTO
  - [ ] 2.1 Create the `event-correlation/` directory structure and `CorrelationResultDto`
    - Create `app/backend/src/onchain/event-correlation/dto/correlation-result.dto.ts` exporting the `CorrelationResult` interface (`fetched`, `correlated`, `inserted`, `updated`, `startLedger`, `endLedger`, `error?`)
    - Create empty barrel files / stubs for `event-mapper.ts`, `event-correlation.service.ts`, `event-correlation.controller.ts` so the module can be wired before full implementation
    - _Requirements: 5.4_

- [ ] 3. Implement `EventMapper`
  - [ ] 3.1 Implement `EventMapper` class with `map()` method
    - Define `RawSorobanEvent` and `OnChainEventPayload` interfaces in `event-mapper.ts`
    - Extract `txHash`, `ledger`, `contractId` directly from the raw event
    - Decode `topic` via `scValToNative(event.topic[0])` with fallback to `"unknown"`
    - Scan `topic[1..n]` and decoded `value` for `"package_id"` / `"id"` (on package events) and `"claim_id"` keys
    - Perform `prisma.aidPackage.findFirst` / `prisma.claim.findUnique` lookups and set FK to null if not found
    - Store `scValToNative(event.value)` as `rawPayload`; set `correlatedAt` to `new Date()`
    - Wrap the entire `map()` body in try/catch; on error log a warning and return payload with both FKs null
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

  - [ ]* 3.2 Write property tests for `EventMapper` (P1–P4)
    - **Property 1: Mapper always extracts required fields from valid events**
      - Use `validSorobanEventArb` fast-check arbitrary; assert `txHash`, `ledger`, `topic`, `contractId` are non-null/non-empty and match input
      - **Validates: Requirements 3.1, 9.1**
    - **Property 2: Mapper produces null foreign keys for events with no package_id or claim_id**
      - Generate events whose `topic` and decoded `value` contain no `"package_id"` or `"claim_id"` keys; assert `packageId === null && claimId === null`
      - **Validates: Requirements 3.5, 9.2**
    - **Property 3: Mapper never throws on malformed input**
      - Use `malformedEventArb` (null, `{}`, wrong types, arbitrary objects); assert `map()` always returns without throwing
      - **Validates: Requirements 9.3**
    - **Property 4: Mapper output round-trips through JSON without data loss**
      - For any valid output of `map()`, assert `JSON.parse(JSON.stringify(payload))` produces a structurally equivalent object
      - **Validates: Requirements 1.1, 9.4**
    - Configure each `fc.assert` with `{ numRuns: 100 }`
    - File: `app/backend/src/onchain/__tests__/event-mapper.spec.ts`

  - [ ]* 3.3 Write example unit tests for `EventMapper` (E1–E3)
    - **E1:** Mapper extracts `package_id` from a well-formed `package_created` fixture event; assert `packageId` equals the expected string
    - **E2:** Mapper extracts `claim_id` from a well-formed `package_claimed` fixture event; assert `claimId` equals the expected CUID
    - **E3:** Mapper returns `packageId = null` when `prisma.aidPackage.findFirst` returns null (mock the DB)
    - _Requirements: 3.2, 3.4, 9.1_

- [ ] 4. Implement `EventCorrelationService`
  - [ ] 4.1 Implement core `correlate()` method and cursor helpers
    - Inject `PrismaService`, `EventMapper`, and `ConfigService`
    - Implement `getCursor()` reading `CorrelationCursor` where `key = "default"`; default to `latestLedger - 1` on first run
    - Implement `setCursor(ledger: number)` upserting `CorrelationCursor` with `key = "default"`
    - Implement `correlate(startLedger?, endLedger?)` — initialize `SorobanRpc.Server`, call `getEvents` with `contractId` filter and `pageSize` from config, iterate events, call `EventMapper.map()` per event, upsert each via `prisma.onChainEvent.upsert` on `(txHash, topic)`, count `inserted` vs `updated`, update cursor to `max(ledger)`, return `CorrelationResult`
    - Handle RPC errors by logging and returning `CorrelationResult` with `error` field set (do not update cursor on RPC failure)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 4.1, 4.2, 4.3, 6.2, 6.3, 6.5, 10.1, 10.2, 10.4_

  - [ ] 4.2 Add scheduled correlation job with concurrency guard
    - Add `private isRunning = false` flag to the service class
    - Add `@Cron('*/5 * * * *', { name: 'event-correlation' })` on `scheduledCorrelate()` method
    - In `scheduledCorrelate()`, check `isRunning` and log `WARN` + return immediately if true; otherwise set `isRunning = true`, call `correlate()`, set `isRunning = false` in a `finally` block
    - In `onModuleInit()`, if `CORRELATION_CRON_EXPRESSION` differs from default, use `SchedulerRegistry` to update the cron job's time
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 10.1, 10.3_

  - [ ]* 4.3 Write example unit tests for `EventCorrelationService` (E4–E5)
    - **E4:** Returns `{ fetched: 0, correlated: 0, inserted: 0, updated: 0 }` when RPC returns empty events array (mock `SorobanRpc.Server`)
    - **E5:** Calling correlate twice with the same single-event RPC response results in `inserted=1, updated=0` on first call and `inserted=0, updated=1` on second call (mock Prisma upsert to track calls)
    - File: `app/backend/src/onchain/__tests__/event-correlation.service.spec.ts`
    - _Requirements: 2.4, 4.1, 4.3_

- [ ] 5. Checkpoint — Ensure all tests pass
  - Run `npx jest --testPathPattern="event-mapper|event-correlation" --run` from `app/backend/`
  - Ensure all P1–P4 property tests and E1–E5 example tests pass; ask the user if any questions arise.

- [ ] 6. Implement `EventCorrelationController`
  - [ ] 6.1 Implement `POST /v1/onchain/events/correlate` endpoint
    - Create `EventCorrelationController` with `@Controller('v1/onchain/events')` decorator
    - Add `@Post('correlate')` handler accepting `@Query('startLedger')` and `@Query('endLedger')` as optional strings
    - Parse each to integer; throw `BadRequestException` if `startLedger` is provided and `<= 0` or `NaN`
    - Throw `BadRequestException` if `endLedger < startLedger`
    - Delegate to `EventCorrelationService.correlate()`; map RPC connectivity errors to `HttpException(502)`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [ ]* 6.2 Write example unit tests for `EventCorrelationController` (E6–E8)
    - **E6:** `POST /v1/onchain/events/correlate?startLedger=0` returns HTTP 400
    - **E7:** `POST /v1/onchain/events/correlate?startLedger=100&endLedger=50` returns HTTP 400
    - **E8:** `POST /v1/onchain/events/correlate` returns HTTP 502 when the service throws an RPC network error
    - File: `app/backend/src/onchain/__tests__/event-correlation.service.spec.ts` (add controller describe block)
    - _Requirements: 5.2, 5.3, 5.5_

- [ ] 7. Wire `EventCorrelationModule` into `OnchainModule`
  - [ ] 7.1 Register new providers and controller in `onchain.module.ts`
    - Import `ScheduleModule` and check if already present in `AppModule`; add `ScheduleModule.forRoot()` to `OnchainModule` imports only if not at app root
    - Add `EventCorrelationService` and `EventMapper` to `providers`
    - Add `EventCorrelationController` to `controllers`
    - Export `EventCorrelationService` from the module
    - _Requirements: 10.1_

- [ ] 8. Update `ClaimsService` to include `onChainEvents`
  - [ ] 8.1 Modify `findOne` in `claims.service.ts` to fetch and return correlated events
    - After the existing `prisma.claim.findUnique`, add a `prisma.onChainEvent.findMany` call with `where: { claimId: id }`, selecting `txHash`, `ledger`, `topic`, `correlatedAt`, `contractId`, `rawPayload`, ordered by `ledger asc`
    - Spread the result into the returned object as `onChainEvents` (always an array, never undefined)
    - _Requirements: 7.1, 7.2, 7.3_

  - [ ]* 8.2 Write example unit test for claim endpoint (E9)
    - **E9:** `GET /v1/claims/:id` response includes `onChainEvents: []` when no correlated events exist for the claim (mock `prisma.onChainEvent.findMany` to return `[]`)
    - _Requirements: 7.1, 7.2_

- [ ] 9. Update `AidEscrowController` to include `onChainEvents`
  - [ ] 9.1 Inject `PrismaService` into `AidEscrowController` and update `getAidPackage`
    - Add `PrismaService` to `AidEscrowModule`'s providers list and inject it into `AidEscrowController` constructor
    - Modify the `getAidPackage` handler to run `aidEscrowService.getAidPackage()` and `prisma.onChainEvent.findMany({ where: { packageId } })` concurrently via `Promise.all`
    - Spread `onChainEvents` into the returned object (always an array, never undefined)
    - _Requirements: 8.1, 8.2, 8.3_

  - [ ]* 9.2 Write example unit test for package endpoint (E10)
    - **E10:** `GET /v1/aid/packages/:id` response includes a populated `onChainEvents` array when the mock `prisma.onChainEvent.findMany` returns two event rows with `txHash`, `ledger`, `topic`, `correlatedAt`
    - _Requirements: 8.1, 8.3_

- [ ] 10. Final checkpoint — Ensure all tests pass
  - Run `npx jest --testPathPattern="event-mapper|event-correlation" --run` from `app/backend/`
  - Ensure all P1–P4 property tests and E1–E10 example tests pass; ask the user if any questions arise.

---

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP
- Each task references specific requirements for traceability
- The design uses TypeScript/NestJS throughout — all code examples should follow existing project conventions
- `ScheduleModule.forRoot()` must be registered only once; check `app.module.ts` before adding it to `OnchainModule`
- Property tests use `fast-check` with `{ numRuns: 100 }` per property
- `SorobanRpc.Server.getEvents` is used directly (not via the existing `OnchainAdapter`)
- Both `packageId` and `claimId` on `OnChainEvent` are nullable strings (not Prisma relations) to avoid FK constraint failures

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "2.1"] },
    { "id": 2, "tasks": ["3.1"] },
    { "id": 3, "tasks": ["3.2", "3.3", "4.1"] },
    { "id": 4, "tasks": ["4.2", "4.3"] },
    { "id": 5, "tasks": ["6.1", "7.1"] },
    { "id": 6, "tasks": ["6.2", "8.1", "9.1"] },
    { "id": 7, "tasks": ["8.2", "9.2"] }
  ]
}
```
