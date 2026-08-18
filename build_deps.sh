#!/usr/bin/env bash
# Zip the common/ package for deployment to Glue via --extra-py-files.
#
# Usage:
#   ./build_deps.sh
#   aws s3 cp pinterest_common.zip s3://<your-bucket>/pinterest_common.zip
#
# Then set, on each of the three jobs in jobs/:
#   --extra-py-files s3://<your-bucket>/pinterest_common.zip
#
# The zip's root must contain the `common` folder itself (not its contents
# directly) so that `import common.auth` etc. resolves once Glue adds the zip
# to sys.path.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

rm -f pinterest_common.zip
zip -r pinterest_common.zip common -x '*__pycache__*'

echo "Built pinterest_common.zip"