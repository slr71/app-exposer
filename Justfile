dockerfile := "Dockerfile"
image-name := "harbor.cyverse.org/de/app-exposer"
tag := "latest"
platform := "linux/amd64"
build-context := "."
container-runtime := "docker"
build-flags := ""

default: build

build: docs operator-docs app-exposer vice-operator vice-operator-tool vice-operator-token workflow-builder vice-export vice-import vice-launch vice-list vice-bundle vice-userid

build-image:
    {{ container-runtime }} build -f {{ build-context}}/{{ dockerfile }} -t {{ image-name }}:{{ tag }} --platform {{ platform }} {{ build-flags }} {{ build-context}}

push:
    {{ container-runtime }} push {{ image-name }}:{{ tag }}

write-build-file output-file="build.json":
    #!/usr/bin/env bash
    set -euo pipefail

    RUNTIME="{{ container-runtime }}"
    IMAGE_NAME="{{ image-name }}"
    IMAGE_TAG="{{ tag }}"
    OUTPUT_FILE="{{ output-file }}"
    FULL_IMAGE_TAG="${IMAGE_NAME}:${IMAGE_TAG}"

    echo "Extracting sha256 digest from built image: ${FULL_IMAGE_TAG}"
    SHA256_DIGEST=$($RUNTIME inspect --format='{{ "{{" }}index .RepoDigests 0{{ "}}" }}' "$FULL_IMAGE_TAG" 2>/dev/null || true)

    # If RepoDigests is empty (image not pushed), get the image ID instead.
    if [[ -z "$SHA256_DIGEST" ]]; then
        IMAGE_ID=$($RUNTIME inspect --format='{{ "{{" }}.Id{{ "}}" }}' "$FULL_IMAGE_TAG" | cut -d: -f2)
        if [[ -n "$IMAGE_ID" ]]; then
            SHA256_DIGEST="${IMAGE_NAME}@sha256:${IMAGE_ID}"
            echo "Using local image digest: $SHA256_DIGEST"
        else
            echo "Error: Failed to extract image digest for ${FULL_IMAGE_TAG}" >&2
            exit 1
        fi
    else
        echo "Using repo digest: $SHA256_DIGEST"
    fi

    echo "Writing build JSON to: $OUTPUT_FILE"
    cat > "$OUTPUT_FILE" << EOF
    {
      "builds": [
        {
          "imageName": "$IMAGE_NAME",
          "tag": "$SHA256_DIGEST"
        }
      ]
    }
    EOF

app-exposer:
    go build -o bin/app-exposer cmd/app-exposer/*.go

vice-operator:
    go build -o bin/vice-operator cmd/vice-operator/*.go

vice-operator-tool:
    go build -o bin/vice-operator-tool cmd/vice-operator-tool/*.go

vice-operator-token:
    go build -o bin/vice-operator-token cmd/vice-operator-token/*.go

workflow-builder:
    go build -o bin/workflow-builder cmd/workflow-builder/*.go

vice-export:
    go build -o bin/vice-export cmd/vice-export/*.go

vice-import:
    go build -o bin/vice-import cmd/vice-import/*.go

vice-launch:
    go build -o bin/vice-launch cmd/vice-launch/*.go

vice-list:
    go build -o bin/vice-list cmd/vice-list/*.go

vice-bundle:
    go build -o bin/vice-bundle cmd/vice-bundle/*.go

vice-userid:
    go build -o bin/vice-userid cmd/vice-userid/*.go

test-imageinfo:
    go test ./imageinfo

test-common:
    go test ./common

test-db:
    go test ./db/...

test-expiration:
    go test ./expiration/...

test-iplantgroups:
    go test ./iplantgroups/...

test-notifications:
    go test ./notifications/...

test-operator:
    go test ./operator/...

test-operatorclient:
    go test ./operatorclient/...

test-app-exposer:
    go test ./cmd/app-exposer/...

test-vice-operator:
    go test ./cmd/vice-operator/...

test-vice-operator-tool:
    go test ./cmd/vice-operator-tool/...

test-vice-export:
    go test ./cmd/vice-export/...

test-vice-import:
    go test ./cmd/vice-import/...

test-vice-launch:
    go test ./cmd/vice-launch/...

test-vice-list:
    go test ./cmd/vice-list/...

test-vice-bundle:
    go test ./cmd/vice-bundle/...

test-vice-userid:
    go test ./cmd/vice-userid/...

test: test-imageinfo test-common test-db test-expiration test-iplantgroups test-notifications test-operator test-operatorclient test-app-exposer test-vice-operator test-vice-operator-tool test-vice-export test-vice-import test-vice-launch test-vice-list test-vice-bundle test-vice-userid

fmt-docs:
    swag fmt -g app.go -d cmd/app-exposer/,httphandlers/,common/,incluster/

docs: fmt-docs
    swag init --parseDependency -g app.go -d cmd/app-exposer/,httphandlers/,common/,incluster/

fmt-operator-docs:
    swag fmt -g app.go -d cmd/vice-operator/,operator/,operatorclient/,common/

operator-docs: fmt-operator-docs
    # Use [[/]] delimiters because the operator's API surfaces Gateway API
    # CRD types whose godoc contains literal Go-template-style braces in
    # kubebuilder default annotations, which break Go template parsing when
    # the default delimiters are used.
    # InstanceName must match the echoSwagger handler in cmd/vice-operator/app.go.
    swag init --parseDependency -g app.go -d cmd/vice-operator/,operator/,operatorclient/,common/ -o operatordocs/ --instanceName operator --templateDelims '[[,]]'

clean:
    #!/usr/bin/env bash
    go clean
    if [ -f bin/app-exposer ]; then
        rm bin/app-exposer
    fi
    if [ -f bin/vice-operator ]; then
        rm bin/vice-operator
    fi
    if [ -f bin/workflow-builder ]; then
        rm bin/workflow-builder
    fi
    if [ -f bin/vice-export ]; then
        rm bin/vice-export
    fi
    if [ -f bin/vice-import ]; then
        rm bin/vice-import
    fi
    if [ -f bin/vice-launch ]; then
        rm bin/vice-launch
    fi
    if [ -f bin/vice-list ]; then
        rm bin/vice-list
    fi
    if [ -f bin/vice-bundle ]; then
        rm bin/vice-bundle
    fi
    if [ -f bin/vice-userid ]; then
        rm bin/vice-userid
    fi
    if [ -f bin/vice-operator-token ]; then
        rm bin/vice-operator-token
    fi
