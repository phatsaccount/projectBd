#!/usr/bin/env bash
# Creates or recreates the movies index using the indexer
set -euo pipefail
PYTHON=${PYTHON:-python}
$PYTHON -c "import sys; print('Using', sys.executable)"
$PYTHON backend/app/infrastructure/elasticsearch/indexer.py

echo "Index setup invoked"
