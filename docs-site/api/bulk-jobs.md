---
title: Bulk Jobs
description: Async bulk execution model, supported operations, limits, and result retrieval.
---

# Bulk Jobs

Bulk jobs exist for cases where synchronous bulk endpoints are too small or too awkward to keep open. The request is accepted, persisted in Postgres, and processed by the worker.

## When To Use Jobs

Use a bulk job when:

- the batch exceeds the synchronous limit of 250 identifiers
- the operation is long-running or expensive
- you want a durable record of the request and result

Use the synchronous bulk endpoints when the client needs immediate results and the payload is small. See [API overview](overview.md).

## Supported Operations

The worker and API currently support these async operations:

- `titles.bulk.get`
- `titles.bulk.resolve`
- `ratings.bulk`
- `series.bulk.episode-ratings`
- `episodes.bulk`
- `names.bulk`

Each operation has a dedicated payload validator. Invalid operations or malformed payloads are rejected before the job is created.

## Job Creation

Create a job with:

`POST /v1/bulk/jobs`

The request body must include:

- `operation`
- `payload`

Example:

```bash
curl -X POST https://api.example.com/v1/bulk/jobs \
  -H "X-API-Key: <key>" \
  -H "Content-Type: application/json" \
  -d '{
    "operation": "titles.bulk.get",
    "payload": {
      "identifiers": ["tt27444205", "tt32459853"]
    }
  }'
```

The API returns `202 Accepted` with a bulk job document. The `resultUrl` appears after the job succeeds.

## Job Lifecycle

1. The API validates the request payload and writes a `queued` job row.
2. The worker claims the next queued job.
3. The worker executes the matching bulk operation.
4. The worker stores the serialized result and marks the job complete or failed.
5. The result remains queryable until the job expires.

The job expiry is currently set to seven days from creation.

## Result Retrieval

Use these routes:

- `GET /v1/bulk/jobs/{jobId}` for status and metadata
- `GET /v1/bulk/jobs/{jobId}/result` for the stored result envelope

Do not assume the result endpoint is always present immediately after creation. Query the job first and retry until the status is terminal.

## Payload Limits

- synchronous bulk endpoints: 250 identifiers
- async bulk jobs: 10,000 identifiers

The validators also reject empty arrays and blank items. That is intentional and should be treated as client input validation, not a transient failure.

## Failure Modes

If a job fails, the underlying service error is stored in the job record. If the payload itself is invalid, the server rejects the request before the job is created.

For operational handling and retries, read [Operations runbook](../operations/runbook.md).

