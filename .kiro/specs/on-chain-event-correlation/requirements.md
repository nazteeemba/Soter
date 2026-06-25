# Requirements Document

## Introduction

The On-Chain Event Correlation Service extends the Soter backend to automatically match Soroban smart contract events to internal `AidPackage` and `Claim` records. When the AidEscrow contract emits events (package creation, claim, disburse, etc.), the service correlates them to the corresponding internal records by `package_id` / `claim_id`, then persists the mapping — transaction hash, ledger number, and event topic — to a dedicated `OnChainEvent` table. Correlated data is then surfaced through the existing claim and package read endpoints. Correlation can be triggered on-demand via an API call and/or runs automatically on a scheduled background job.

---

## Glossary

- **Correlator**: The internal NestJS service (`EventCorrelationService`) responsible for fetching Soroban events and linking them to internal records.
- **OnChainEvent**: The Prisma model that stores a correlated event record (tx hash, ledger, topic, internal entity reference).
- **Soroban_RPC**: The Stellar Soroban JSON-RPC endpoint used to query contract events.
- **package_id**: The numeric identifier of an `AidPackage` record, as referenced both on-chain and internally.
- **claim_id**: The CUID identifier of a `Claim` record stored in the Soter database.
- **event_topic**: A string label emitted by the AidEscrow contract (e.g., `"package_created"`, `"package_claimed"`, `"package_disbursed"`).
- **Mapper**: The pure function / class within the Correlator that transforms a raw Soroban event into an `OnChainEvent` payload.
- **Scheduler**: The NestJS `@Cron`-driven job inside `EventCorrelationService` that runs correlation periodically.
- **EventCorrelationModule**: The NestJS module that wires together the Correlator, Mapper, controller, and scheduler.
- **Cursor**: The last processed ledger number persisted to storage so the Scheduler can resume from where it left off.

---

## Requirements

### Requirement 1: Persistent On-Chain Event Storage

**User Story:** As a platform operator, I want correlated Soroban events persisted to the database, so that I have an auditable, queryable record of every on-chain action tied to internal records.

#### Acceptance Criteria

1. THE `OnChainEvent` model SHALL store the following fields for each correlated event: `id`, `txHash`, `ledger`, `topic`, `contractId`, `packageId` (nullable), `claimId` (nullable), `rawPayload` (JSON), `correlatedAt`.
2. THE `OnChainEvent` model SHALL enforce a unique constraint on `txHash` combined with `topic` to prevent duplicate records.
3. WHEN an `OnChainEvent` record is created, THE database SHALL index on `packageId`, `claimId`, `ledger`, and `correlatedAt` to support efficient look-ups; IF index creation fails due to database constraints or permissions, THE system SHALL still allow the event record to be created.
4. THE `OnChainEvent` model SHALL allow `packageId` and `claimId` to both be null to support events that cannot be immediately correlated.

---

### Requirement 2: Event Fetching from Soroban RPC

**User Story:** As a platform operator, I want the service to fetch Soroban events from the RPC endpoint, so that correlation always works against fresh on-chain data.

#### Acceptance Criteria

1. WHEN correlation is triggered, THE Correlator SHALL call the Soroban RPC `getEvents` method with the configured `contractId` and a ledger range `[startLedger, latestLedger]`.
2. WHEN the Soroban RPC returns a non-2xx response or a JSON-RPC error, THE Correlator SHALL log the error and return a structured error result without throwing an unhandled exception.
3. THE Correlator SHALL apply a configurable page size (default 200) when fetching events to avoid exceeding RPC payload limits.
4. WHEN the RPC response contains zero events, THE Correlator SHALL return a result with `fetched: 0, correlated: 0` without writing any database records.

---

### Requirement 3: Event-to-Record Mapping

**User Story:** As a platform operator, I want raw Soroban events to be mapped to internal package and claim records, so that on-chain activity is traceable to business objects.

#### Acceptance Criteria

1. WHEN the Mapper receives a raw Soroban event, THE Mapper SHALL extract `txHash`, `ledger`, `topic`, `contractId`, and any `package_id` / `claim_id` values present in the event's topic or value fields.
2. WHEN a `package_id` is extracted, THE Mapper SHALL look up the corresponding `AidPackage` record by matching `package_id` as a string-cast numeric ID.
3. WHEN a `package_id` is extracted but no matching `AidPackage` record exists in the database, THE Mapper SHALL set the `packageId` foreign key to null and continue processing the event.
4. WHEN a `claim_id` is extracted, THE Mapper SHALL look up the corresponding `Claim` record by matching `claim_id` as a CUID string.
5. WHEN neither a `package_id` nor a `claim_id` is found in an event, THE Mapper SHALL still produce an `OnChainEvent` record with both foreign keys set to null.
6. THE Mapper SHALL be a pure, dependency-injectable class with no side effects beyond returning a mapped payload, enabling isolated unit testing.

---

### Requirement 4: Idempotent Upsert Persistence

**User Story:** As a platform operator, I want repeated correlation runs to be safe, so that re-running the job never creates duplicate records.

#### Acceptance Criteria

1. WHEN the Correlator persists a mapped event, THE Correlator SHALL use an upsert keyed on `(txHash, topic)` so that re-processing the same event produces exactly one database row.
2. WHEN an upsert matches an existing record, THE Correlator SHALL update the `rawPayload` and `correlatedAt` fields and leave all other fields unchanged.
3. WHEN the Correlator processes a batch of events, THE Correlator SHALL report the count of newly inserted records and the count of updated records separately.

---

### Requirement 5: On-Demand Correlation Endpoint

**User Story:** As a platform operator, I want to trigger correlation manually via an API call, so that I can backfill or re-correlate events without waiting for the next scheduled run.

#### Acceptance Criteria

1. THE `EventCorrelationModule` SHALL expose a `POST /v1/onchain/events/correlate` endpoint that accepts optional `startLedger` and `endLedger` query parameters.
2. WHEN `startLedger` is provided and is not a positive integer, THE endpoint SHALL return HTTP 400 with a descriptive error message.
3. WHEN `endLedger` is provided and is less than `startLedger`, THE endpoint SHALL return HTTP 400 with a descriptive error message.
4. WHEN the on-demand correlation completes and counts can be reported, THE endpoint SHALL return HTTP 200 with a JSON body containing `fetched`, `correlated`, `inserted`, and `updated` counts, regardless of whether individual correlation operations partially failed.
5. WHEN the Soroban RPC is unreachable during an on-demand request, THE endpoint SHALL return HTTP 502 with a structured error body.

---

### Requirement 6: Scheduled Correlation Job

**User Story:** As a platform operator, I want correlation to run automatically on a schedule, so that on-chain events are regularly imported without manual intervention.

#### Acceptance Criteria

1. THE Scheduler SHALL run the correlation job every 5 minutes using a cron expression equivalent to `*/5 * * * *`.
2. WHEN the Scheduler runs, THE Scheduler SHALL start from the last persisted Cursor ledger and end at the latest ledger returned by the Soroban RPC.
3. WHEN correlation completes, THE Scheduler SHALL update the Cursor to the value of the highest processed ledger.
4. WHEN a scheduled run is already in progress, THE Scheduler SHALL skip the new trigger and log a warning to avoid concurrent execution.
5. IF the Scheduler encounters a fatal error during a run, THEN THE Scheduler SHALL log the error with structured context (ledger range, error message) and update the Cursor to the highest successfully processed ledger before the error, so the next run resumes from that point.

---

### Requirement 7: Correlated Data on Claim Read Endpoint

**User Story:** As an API consumer, I want to see on-chain events when reading a claim, so that I can verify the blockchain state without a separate request.

#### Acceptance Criteria

1. WHEN a `GET /v1/claims/:id` response is assembled, THE Claims_Controller SHALL always include an `onChainEvents` field in the response structure, containing all `OnChainEvent` records whose `claimId` matches the requested claim's `id`.
2. WHEN no `OnChainEvent` records exist for a claim, THE Claims_Controller SHALL return an empty `onChainEvents` array rather than omitting the field.
3. THE `onChainEvents` array SHALL contain objects with at minimum `txHash`, `ledger`, `topic`, and `correlatedAt` fields.

---

### Requirement 8: Correlated Data on Package Read Endpoint

**User Story:** As an API consumer, I want to see on-chain events when reading an aid package, so that I can verify the blockchain state without a separate request.

#### Acceptance Criteria

1. WHEN a `GET /v1/aid/packages/:id` response is assembled, THE AidEscrow_Controller SHALL always include an `onChainEvents` field in the response structure, containing all `OnChainEvent` records whose `packageId` matches the requested package's `id`.
2. WHEN no `OnChainEvent` records exist for a package, THE AidEscrow_Controller SHALL return an empty `onChainEvents` array rather than omitting the field.
3. THE `onChainEvents` array items SHALL include at minimum `txHash`, `ledger`, `topic`, and `correlatedAt` fields.

---

### Requirement 9: Mapper Unit Tests

**User Story:** As a developer, I want the Mapper logic covered by unit tests, so that regressions in event parsing are caught before deployment.

#### Acceptance Criteria

1. THE test suite SHALL include unit tests for the Mapper that verify correct extraction of `txHash`, `ledger`, `topic`, `package_id`, and `claim_id` from a well-formed Soroban event fixture.
2. THE test suite SHALL include unit tests that verify the Mapper returns null foreign keys when neither `package_id` nor `claim_id` is present in the event.
3. THE test suite SHALL include unit tests that verify the Mapper handles malformed or incomplete event payloads without throwing an unhandled exception.
4. THE test suite SHALL include a round-trip property: FOR ALL valid `OnChainEvent` payloads produced by the Mapper, serializing the payload to JSON and deserializing it SHALL produce an equivalent object.
5. THE test suite SHALL be runnable via the existing `jest` test command in the `backend` package without any additional configuration.

---

### Requirement 10: Configuration and Environment Variables

**User Story:** As a developer, I want all external dependencies of the correlation service to be configurable via environment variables, so that the service can target different networks without code changes.

#### Acceptance Criteria

1. THE `EventCorrelationModule` SHALL read `SOROBAN_RPC_URL`, `AID_ESCROW_CONTRACT_ID`, and `CORRELATION_CRON_EXPRESSION` from the application's `ConfigService`.
2. WHEN `SOROBAN_RPC_URL` is not set, THE `EventCorrelationModule` SHALL throw a configuration error at application startup that prevents the service from starting.
3. WHERE `CORRELATION_CRON_EXPRESSION` is not set, THE Scheduler SHALL default to `*/5 * * * *`.
4. THE `EventCorrelationModule` SHALL expose a `CORRELATION_PAGE_SIZE` configuration variable (default 200) that controls the number of events fetched per RPC call.
