# Realtime Product Systems Lab

Keep a developer-facing state feed coherent across reconnects and slow consumers.

[![CI](https://github.com/afimeth/realtime-product-systems-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/afimeth/realtime-product-systems-lab/actions)

## Run, test and demo

```sh
python app.py demo
python app.py serve
python scripts/verify.py
```

Install Python 3.11+ for the evidence harness. Python implementation uses only the standard library. External paid services are not required.

## Implemented

Asyncio loopback TCP JSON-lines server, live sequence-numbered events, bounded replay history, snapshot fallback, bounded subscriber queues, slow-consumer eviction, state/subscriber caps, CLI demo and metrics.

## Evidence and status

- **IMPLEMENTED:** runnable code and failure tests in this repository.
- **MEASURED:** [baseline](evidence/baseline.json) identifies the measured source commit. Each CI matrix job uploads a fresh `receipt.json` for its exact `GITHUB_SHA`, with actual test totals and toolchain. A receipt commit does not rewrite the measured source SHA.
- **DESIGNED:** WebSocket/browser integration and durable multi-process event log remain extensions.
- **NOT CLAIMED:** No WebSocket, ASGI, WebRTC, media transport, browser subscription UI, durable restart state, multi-process fanout, auth/TLS, distributed ordering, or internet deployment. StreamWriter/OS buffering is separate from application queue bounds. Active pre-subscription sockets have a two-second timeout but no global connection cap. Core event objects are trusted in-process data.

Run `python scripts/verify.py` to regenerate ignored local receipts. Benchmark/gas/bundle reports are local measurements, not a production SLO. CI and local runs are separate evidence. Passing tests are not an independent review or an accepted production release.

## Safe CV claim

> Built and tested a local async state feed with cursor replay, snapshot recovery, bounded slow-consumer queues, and observable CLI metrics.

This describes a laboratory project. It does not establish years of experience, a degree, production scale, employer history, CVEs, mainnet ownership, or independent audit credentials.

## Design and interview walkthrough

See [architecture](docs/ARCHITECTURE.md), [limitations](docs/LIMITATIONS.md), and [interview scenarios](docs/INTERVIEW_SCENARIOS.md). All inputs are synthetic. This is a fresh AI-assisted standalone implementation from public requirements; no private source, customer data, credentials or proprietary code was copied. MIT license applies to this lab.
