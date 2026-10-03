# Limitations and unsupported claims

No WebSocket, ASGI, WebRTC, media transport, browser subscription UI, durable restart state, multi-process fanout, auth/TLS, distributed ordering, or internet deployment. StreamWriter/OS buffering is separate from application queue bounds. Active pre-subscription sockets have a two-second timeout but no global connection cap. Core event objects are trusted in-process data.

Tests exercise fixtures, not production traffic. Measurements include environment and exact commit in the receipt. A failure outcome is distinct from an unmeasured property. No years of experience, degrees, CVEs, production scale, mainnet ownership or independent audit are claimed.
