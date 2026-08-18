#!/usr/bin/env bash
# Zip the common/ modules for deployment to Glue via --extra-py-files.
#
# Usage:
#   ./build_deps.sh
#   aws s3 cp pinterest_common.zip s3://<your-bucket>/pinterest_common.zip
#
# Then set, on each of the five jobs in jobs/:
#   --extra-py-files s3://<your-bucket>/pinterest_common.zip
#
# The zip's root must contain the pinterest_*.py files directly, with NO
# wrapping "common" folder -- see README.md's "Deploying" section for why:
# a package-with-__init__.py zip (the officially documented alternative
# structure) hit ModuleNotFoundError on a real Glue job run, matching a
# known unresolved AWS Glue issue with zipimport + --extra-py-files. Flat
# files avoid that machinery entirely -- each becomes directly importable
# once Glue adds this zip to sys.path.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

rm -f pinterest_common.zip
(cd common && zip -r ../pinterest_common.zip . -x '*__pycache__*')

echo "Built pinterest_common.zip"
