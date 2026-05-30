# Automated Disaster Recovery and Multi-Region Failover

This document describes the keeper's automated disaster recovery path for Soroban RPC outages.

## Goals

- Keep task polling and execution available when a primary region fails.
- Detect endpoint degradation early and route traffic to healthy regions.
- Emit explicit operational signals for failover events and endpoint health.

## Architecture

The keeper now uses `MultiRegionRPCClient` as the RPC access layer.

- Endpoints are configured as an ordered region list.
- One endpoint is marked active at any point in time.
- Calls are attempted on the active endpoint first.
- On failure, calls automatically retry across the remaining healthy endpoints.
- Repeated failures mark an endpoint unavailable for a cooldown window.
- Background health checks periodically recover endpoints and may rebalance active routing.

## Configuration

Required:

- `SOROBAN_RPC_URL`: primary RPC URL (backward-compatible default)

Optional failover controls:

- `SOROBAN_RPC_URLS`: comma-separated list of additional RPC URLs (multi-region)
- `RPC_FAILOVER_ENABLED`: `true|false` (default: enabled when more than one URL exists)
- `RPC_FAILOVER_FAILURE_THRESHOLD`: consecutive failures before endpoint quarantine (default: `3`)
- `RPC_FAILOVER_COOLDOWN_MS`: endpoint cooldown window (default: `30000`)
- `RPC_FAILOVER_HEALTH_CHECK_INTERVAL_MS`: background probe interval (default: `15000`)

## Observability

The metrics and health endpoints include failover state.

JSON endpoints (`/health`, `/metrics`):

- Active region and endpoint index
- Healthy endpoint count versus total
- Per-endpoint status and failure metadata

Prometheus metrics:

- `keeper_rpc_failover_events_total`
- `keeper_rpc_failover_switches_total`
- `keeper_rpc_failover_active_endpoint_index`
- `keeper_rpc_failover_healthy_endpoints`
- `keeper_rpc_failover_total_endpoints`

## Failure and Recovery Flow

1. Active region fails an RPC call.
2. Failure counters increase and endpoint score decreases.
3. If threshold is exceeded, endpoint becomes unavailable for cooldown.
4. Request is retried against alternate regions in-priority order.
5. Successful alternate response updates active endpoint selection.
6. Background health checks recover previously unavailable regions.

## Security Notes

- Keep all RPC URLs on trusted infrastructure and private networking where possible.
- Use TLS endpoints only for production traffic.
- Rotate keeper secrets independently from region failover operations.

## Testing

Unit tests for failover behavior are in `keeper/__tests__/disasterRecovery.test.js` and cover:

- Healthy primary path
- Automatic cross-region failover
- Exhausted failover error path
- Endpoint recovery via health checks
