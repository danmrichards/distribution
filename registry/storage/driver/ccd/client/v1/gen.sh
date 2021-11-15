#!/usr/bin/env bash
set -euo pipefail

codegenCmd='github.com/deepmap/oapi-codegen/cmd/oapi-codegen';
templatesDir="$(dirname "${BASH_SOURCE}")/../templates";

function generate() {
  inputSpecPath="$1";
  inputTarget="$2";
  output="$3";
  package="$4";

  # This will implicitly fetch and compile the third party
  # code generator at the correct version
  go run -mod=mod "${codegenCmd}" \
    -o "${output}" \
    -package "${package}" \
    -templates "${templatesDir}" \
    -generate "${inputTarget}" \
    "${inputSpecPath}";
}

if ! command -v swagger2openapi > /dev/null; then
  echo "Installing swagger2openapi..."
  npm install -g swagger2openapi@7.0.6
fi

if ! command -v yq > /dev/null; then
  echo "Installing yq..."
  go install github.com/mikefarah/yq/v4@latest
fi

script_dir="$(dirname ${BASH_SOURCE})"
# Convert the CCD Swagger doc to OpenAPI, also doing the following:
# - Update API paths to include API base.
# - Switch fromreleaseid and fromreleasenum in GetDiffReleaseEntries from true to false - only one is required, not both.
curl -s "https://content-api.cloud.unity3d.com/doc/doc.json" \
  | swagger2openapi -y /dev/stdin \
  | sed \
      -e 's#"/buckets/#"/api/v1/buckets/#g' \
      -e 's#"/orgs/#"/api/v1/orgs/#g' \
      -e 's#"/users/#"/api/v1/users/#g' \
      -e 's#/users/#/api/v1/users/#g' \
      -e 's#"/projects/#"/api/v1/projects/#g' \
      -e 's#promoteBucketResponse#promoteBucketAPIResponse#g' \
  | yq e '
    .paths."/api/v1/buckets/{bucketid}/diff/releases/entries/".get.parameters[1].required = false |
    .paths."/api/v1/buckets/{bucketid}/diff/releases/entries/".get.parameters[2].required = false
    ' - \
  > "${script_dir}/management.yaml"

generate "${script_dir}/management.yaml" types "${script_dir}/types.go" v1 "../../../../components-base.yaml:github.com/multiplay/go/soa/apis/base/api"
generate "${script_dir}/management.yaml" client "${script_dir}/client.go" v1 "../../../../components-base.yaml:github.com/multiplay/go/soa/apis/base/api"
