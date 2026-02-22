---
title: SolanaCDN (Integrated)
sidebar_position: 50
sidebar_label: SolanaCDN
pagination_label: SolanaCDN (Integrated)
---

This fork includes an **integrated SolanaCDN client** in the validator (no `solanacdn-agent` sidecar).

This is the recommended production path. The `solanacdn-agent` sidecar is deprecated and kept only
for legacy deployments.

## What it does

When enabled, the validator:

- publishes inbound TVU shreds to a SolanaCDN POP (QUIC; optional UDP data plane)
- subscribes to POP shreds and injects them into local TVU/gossip
- optionally requests POP direct injection of raw shred UDP payloads to validator ports (lowest latency)
- tunnels UDP vote packets via the POP mesh (best-effort) and injects POP vote-tunnel responses into
  the validator vote socket when enabled
- posts per-run counters to the Pipe API ingest endpoint (so the Pipe console can show agent-style metrics)

## Enable (Pipe-managed POP discovery)

If you have a Pipe API key, you can enable SolanaCDN without specifying POP endpoints:

- Set `PIPE_API_KEY` (or `SOLANACDN_AGENT_API_TOKEN`) in the environment.
- Run the validator with your normal args, plus:
  - `--solanacdn-api-token <TOKEN>` (optional if using env)
  - (optional) `--solanacdn-only` to ingest only SolanaCDN-sourced shreds when connected (fallback to P2P when disconnected)
  - (optional) `--solanacdn-hybrid` to prefer SolanaCDN shreds when healthy, but fall back to P2P if SolanaCDN stalls while connected (`--solanacdn-hybrid-stale-ms` controls the stall threshold)

The validator verifies the API key via `POST /v1/solanacdn-agent/verify` and uses the returned POP list.

For scalability and blast-radius reduction, the Pipe API can return a **per-node assigned subset** of
active POPs (instead of returning all POPs globally to every client). The validator will only
connect to the returned subset.

If the Pipe API returns POP pubkeys (`pop_endpoints_v2`), the validator can pin the expected POP
pubkey during auth:

- `--solanacdn-pop-pubkey-pinning warn` (default): log mismatch, continue
- `--solanacdn-pop-pubkey-pinning enforce`: treat mismatch as fatal and disconnect

## Enable (explicit POP list / private CA)

If you self-host POPs (or want to point at a specific POP directly), configure:

- `--solanacdn-pop <IP:PORT>` (repeatable)
- `--solanacdn-server-name <SNI>` (must match the POP certificate SAN)
- `--solanacdn-tls-ca-cert-path <FILE>` if using a private CA

Dev-only escape hatch:

- `--solanacdn-tls-insecure-skip-verify`

## TLS behavior

- By default, the validator verifies POP TLS certificates using the system/WebPKI trust roots.
- If you provide `--solanacdn-tls-ca-cert-path`, only that CA bundle is trusted for POP verification.
- If `/etc/solanacdn/tls/ca.crt` exists, it is used automatically as the POP/control CA bundle.
- In the open-source build, POP TLS CA bootstrap via the Pipe API is opt-in only.
  Enable with `--solanacdn-api-tls-bootstrap`.

## Vote tunnel dedup tuning

- Vote-tunnel responses are deduplicated by default (TTL 2000ms, 200000 entries).
- Tune with `--solanacdn-vote-dedup-ttl-ms <MILLISECONDS>` (set `0` to disable).
- Tune with `--solanacdn-vote-dedup-max-entries <COUNT>` (set `0` to disable).

## Observability

- Metrics + status: `--solanacdn-metrics-addr HOST:PORT` exposes Prometheus at `/metrics` and JSON status at `/solanacdn/status`.
- Admin RPC: `solanaCdnStatus` returns the same `SolanaCdnStatus` JSON.
- In `--solanacdn-hybrid` mode, `tvu_shred_stale` / `tvu_shred_stale_for_ms` reflect time since the last shred accepted into the validator pipeline (compare with `last_shred_*` to diagnose delivery vs discard).
- Race metrics (SolanaCDN vs gossip): enabled by default; disable with `--solanacdn-race=false`. Tune via `--solanacdn-race-sample-bits` and `--solanacdn-race-window-ms` (compatible with `--solanacdn-only` / `--solanacdn-hybrid`; does not change shred ingest mode).
- Transaction hygiene counters include dedup drops (`solanacdn_tx_deduped_packets_total`).
- Vote-tunnel counters include `solanacdn_rx_vote_packets_total` and `solanacdn_dropped_vote_datagrams_total`.

## Notes / limitations

- Vote tunneling is UDP-only. If validator QUIC votes are enabled, SolanaCDN vote tunneling is disabled automatically.
- `--solanacdn-no-repair` disables repair shreds; this can stall catch-up if SolanaCDN misses shreds. Use with care.
