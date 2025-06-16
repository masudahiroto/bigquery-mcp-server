#!/usr/bin/env bash
set -euo pipefail

# Verify required environment variables
: "${BQ_PROJECT?Need to set BQ_PROJECT}"
: "${BQ_CLIENT_PROJECT?Need to set BQ_CLIENT_PROJECT}" || true
: "${BQ_DATASET?Need to set BQ_DATASET}"
: "${BQ_TABLE?Need to set BQ_TABLE}"
: "${BQ_SQL?Need to set BQ_SQL}"

# Run E2E tests with the e2e build tag
exec go test -tags=e2e ./e2e -v
