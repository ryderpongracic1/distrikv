# Security policy

## Supported versions

Until the first tagged release, security fixes are made on `main`. After
`v0.1.0`, only the latest release line is supported.

## Reporting a vulnerability

Please use GitHub's private vulnerability-reporting feature for this repository.
Do not open a public issue containing exploit details or sensitive data.

## Deployment boundary

distrikv currently has no authentication, authorization, or TLS on its HTTP or
gRPC interfaces. It is a research/portfolio system intended for a trusted local
network. Do not expose ports 8001–8003 or 9001–9003 directly to the internet.

The `/metrics`, `/metrics/prometheus`, `/status`, `/healthz`, and `/readyz`
endpoints are intentionally unauthenticated and may reveal operational state.
