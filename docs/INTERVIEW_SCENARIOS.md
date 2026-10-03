# Interview scenarios

These are inferred practice scenarios from a role family, not leaked or confirmed employer questions.

Build a live developer status feed; disconnect and reconnect at a cursor; overflow replay history; isolate a slow client; explain sequence consistency and restart boundaries.

## Suggested 15-minute walkthrough

1. Explain the concrete user/system problem and failure boundary.
2. Run the demo and one successful path.
3. Run the adversarial suite and explain one rejected path.
4. Discuss the tradeoffs and the unsupported claims.
5. Modify one requirement live and identify what new test/evidence it needs.

## Evidence path

The README describes run commands. `scripts/verify.py` produces exact-commit receipts and actual test totals; CI artifacts add platform coverage.
