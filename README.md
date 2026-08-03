app-exposer
===========

`app-exposer` is a service for the CyVerse Discovery Environment that provides a CRUD API for managing VICE analyses.

This repository also contains `vice-operator`, a lightweight K8s operator that receives pre-built resource bundles from app-exposer and applies them to a local cluster. See [vice-operator](#vice-operator) below.

# Development

## Prerequisites

* `just` - A command runner, similar to but different from Make. See [just](https://github.com/casey/just) for more info.
* `go` - The Go programming language. See [Go](https://go.dev) for more info.
* `swag` - A Swagger 2.0 documentation generator for Go. See [swag](https://github.com/swaggo/swag) for more info.

## Build

You will need [just](https://github.com/casey/just) installed, along with the Go programming language and the bash shell.

To build `app-exposer` alone, run:
```bash
just app-exposer
```

To build `vice-operator` alone, run:
```bash
just vice-operator
```

To build `workflow-builder` alone, run:
```bash
just workflow-builder
```

The repository also ships several admin and development utilities. Build any of them with `just <name>`:

* `vice-operator-tool` — Admin CLI that lists and inspects vice-operator instances via app-exposer's `/vice/admin/operators` endpoint.
* `vice-operator-token` — Reads a JSON config of Keycloak client-credentials settings and prints an access token on stdout, for use in `curl -H "Authorization: Bearer ..."` calls against the vice-operator API. See [API Authentication](#api-authentication).
* `vice-list` — Lists available VICE apps from the DE database.
* `vice-export` — Exports a VICE app definition to JSON.
* `vice-import` — Imports a VICE app definition from JSON.
* `vice-launch` — Launches a VICE analysis from a `vice-export` JSON.
* `vice-bundle` — Generates a vice-operator analysis bundle for testing.
* `vice-userid` — Looks up a user's internal user ID from the DE database.

To build everything, run:
```bash
just
```

To clean your local repo, run:
```bash
just clean
```

Uses Go modules, so make sure you have a generally up-to-date version of Go installed locally.

## Tests

Run all tests:
```bash
just test
```

Run operator-specific tests:
```bash
just test-operator
just test-operatorclient
```

The API documentation is written using the OpenAPI 3 specification and is located in `api.yml`. You can use redoc to view the documentation locally:

Install:
```npm install -g redoc-cli```

Run:
```redoc-cli serve -w api.yml```

For configuration, use `example-config.yml` as a reference. You'll need to either port-forward to or run `job-status-listener` locally and reference the correct port in the config.

## Command-Line Flags (app-exposer)

### Authentication Control

`--disable-vice-proxy-auth` (default: `false`)

Disables authentication in the vice-proxy sidecar containers for VICE applications. When set to `true`, the `--disable-auth` flag is passed to vice-proxy, allowing unauthenticated access to VICE applications. This is intended for development, testing, or scenarios where authentication is handled elsewhere. In production environments, this should remain `false` (the default) to enforce authentication via Keycloak.

## Regenerating the Swagger docs

Both `app-exposer` and `vice-operator` ship Swagger 2.0 docs generated from godoc-style annotations on the handler source. The generators are invoked via the Justfile so the source-file list and any non-default `swag` flags stay in one place:

```bash
just docs           # regenerates docs/{docs.go,swagger.json,swagger.yaml}
just operator-docs  # regenerates operatordocs/operator_*
```

Each target runs `swag fmt` before `swag init`. Do not hand-edit the generated files — rerun the appropriate target instead.

---

# vice-operator

`vice-operator` is a minimal Kubernetes operator that receives pre-built resource bundles from `app-exposer` and applies them to its local cluster. It enables multi-cluster VICE deployments where app-exposer centrally manages analysis lifecycle while operators handle resource application on remote clusters.

## Architecture

```
  Terrain/UI
      │
  app-exposer (QA cluster)
  ├─ DB, Subscriptions, Permissions, Quota
  ├─ Builds K8s resources from model.Job
  ├─ Serializes into AnalysisBundle
  └─ Sends bundle to operator(s)
      │                    │
  vice-operator (QA)  vice-operator (local)
  ├─ Apply resources   ├─ Apply resources
  ├─ Gateway API       ├─ Gateway API
  └─ K8s API only      └─ K8s API only
```

The operator has **no external dependencies** — no database, apps service, permissions, or quota. It only needs a K8s client and an HTTP server.

App-exposer builds all K8s resource objects (Deployment, Service, HTTPRoute, ConfigMaps, PVs, PVCs, PodDisruptionBudget) using its existing builder functions, serializes them into an `AnalysisBundle`, and sends the bundle to the operator via HTTP. The operator applies the resources to its local cluster, transforming routing as needed to attach to the local Gateway.

## Command-Line Flags

The flags below cover what an operator deployer needs day-to-day; this is not an exhaustive list. Run `vice-operator --help` for the full set.

### Core

| Flag | Default | Description |
|------|---------|-------------|
| `--kubeconfig` | `""` (in-cluster) | Path to kubeconfig file. Empty uses in-cluster config. |
| `--namespace` | `vice-apps` | K8s namespace where VICE resources are created. |
| `--port` | `60001` | HTTP listen port. |
| `--max-analyses` | `50` | Maximum concurrent VICE analyses allowed on this cluster. `0` disables the limit (for autoscaling clusters). |
| `--node-label-selector` | `""` | K8s label selector to filter schedulable nodes for capacity calculation. |
| `--gpu-vendor` | `nvidia` | GPU vendor (`nvidia` or `amd`). |
| `--log-level` | `info` | Log level (`debug`, `info`, `warn`, `error`, `fatal`). |

### Gateway

| Flag | Default | Description |
|------|---------|-------------|
| `--gateway-namespace` | `""` | Namespace of the Gateway resource (defaults to `--namespace`). |
| `--gateway-name` | `vice` | Name of the Gateway resource. |
| `--gateway-class-name` | `traefik` | GatewayClass name for the Gateway resource. |
| `--gateway-entrypoint-port` | `8000` | Entrypoint port on the Gateway listener. |
| `--gateway-skip-creation` | `false` | Skip creation of the Gateway resource (use when attaching to a pre-existing Gateway). |

### API Auth

See [API Authentication](#api-authentication) for how these are evaluated.

| Flag | Default | Description |
|------|---------|-------------|
| `--api-auth` | `true` | Enable OIDC JWT Bearer auth for the API. |
| `--api-auth-issuer-url` | `""` | OIDC issuer URL (e.g. `https://keycloak.example.com/realms/cyverse`). |
| `--api-auth-client-id` | `""` | Expected client ID (`azp` claim) for API auth. |
| `--admin-role` | `vice-operator` | Realm role that grants API access (or `ADMIN_ROLE` env var). |
| `--admin-entitlements` | `""` | Comma-separated entitlement-claim values that grant API access (or `ADMIN_ENTITLEMENTS` env var). |

### Swagger UI Authentication

| Flag | Default | Description |
|------|---------|-------------|
| `--swagger-client-id` | `""` | OAuth2 client ID for the Swagger UI login flow (must support authorization code flow in Keycloak). |
| `--swagger-client-secret` | `""` | OAuth2 client secret for the Swagger UI login flow (or `SWAGGER_CLIENT_SECRET` env var). |
| `--swagger-cookie-secret` | `""` | Secret for signing session cookies (random string; auto-generated if empty; or `SWAGGER_COOKIE_SECRET` env var). |

### vice-proxy Keycloak

These configure the Keycloak client used by the vice-proxy sidecar inside each analysis pod, *not* the API auth above.

| Flag | Default | Description |
|------|---------|-------------|
| `--keycloak-base-url` | `""` | Keycloak base URL for vice-proxy auth. |
| `--keycloak-realm` | `""` | Keycloak realm for vice-proxy auth. |
| `--keycloak-client-id` | `""` | OIDC client ID for vice-proxy auth. |
| `--keycloak-client-secret` | `""` | OIDC client secret for vice-proxy auth (or `KEYCLOAK_CLIENT_SECRET` env var). |
| `--disable-vice-proxy-auth` | `false` | Disable auth in vice-proxy (development/testing only). |
| `--ca-bundle-configmap` | `""` | ConfigMap in `--namespace` holding the CA vice-proxy must trust to reach Keycloak; needed only where the DE's certificates chain to a private CA. Mounted read-only at `/etc/de-ca` with `SSL_CERT_FILE` pointed at it. Empty leaves the vice-proxy image's trust store alone. |
| `--ca-bundle-key` | `ca.crt` | Key within `--ca-bundle-configmap` holding the PEM bundle. |

`SSL_CERT_FILE` replaces the vice-proxy image's default bundle file rather than adding to it, so the ConfigMap should hold every CA the sidecar needs — the private CA plus any public roots. The operator validates the ConfigMap and key at startup and exits if either is missing.

The CA bundle only reaches analyses launched through the operator's spec build path. Setting `--disable-spec-launch` routes launches through app-exposer's legacy bundle, which has no CA bundle support; on a private-CA cluster that reverts vice-proxy to failing its Keycloak token exchange. The operator logs a warning at startup when both flags are set.

### Image Pull / Registry

| Flag | Default | Description |
|------|---------|-------------|
| `--image-pull-secret` | `vice-image-pull-secret` | Name of the K8s image pull Secret managed by the operator. |
| `--registry-server` | `""` | Docker registry server (e.g. `harbor.cyverse.org`). |
| `--registry-username` | `""` | Docker registry username. |
| `--registry-password` | `""` | Docker registry password (or `REGISTRY_PASSWORD` env var). |

### Networking

| Flag | Default | Description |
|------|---------|-------------|
| `--vice-base-url` | `https://cyverse.run` | Base URL for VICE; stored in the cluster config secret. |
| `--service-cidr` | `""` | Cluster service CIDR to block in egress (auto-detected from the kubernetes API server when empty). |
| `--disable-internet-access` | `false` | Block analysis pods from reaching the public internet; only DNS, explicit host/CIDR exceptions, and pod exceptions are allowed. |
| `--user-suffix` | `@iplantcollaborative.org` | Domain suffix appended to usernames if not already present. |

### Loading Page

| Flag | Default | Description |
|------|---------|-------------|
| `--loading-port` | `8080` | Listen port for the loading page server. |
| `--loading-service-name` | `vice-operator-loading` | Name of the loading page Service. |
| `--loading-service-port` | `80` | Port of the loading page Service. |
| `--operator-pod-selector` | `""` | Pod selector for vice-operator services (e.g. `app=vice-operator-local`); when set, the operator ensures API and loading Services exist at startup. |

### API HTTPRoute

| Flag | Default | Description |
|------|---------|-------------|
| `--api-subdomain` | `vice-api` | Subdomain prefix for the vice-operator API HTTPRoute; combined with `--vice-base-url`'s host to form the full hostname. |
| `--api-service-name` | `vice-operator` | K8s Service name for the vice-operator API HTTPRoute backend. |

## Running Locally

```bash
just vice-operator

./bin/vice-operator \
  --kubeconfig=$HOME/.kube/local-admin.conf \
  --namespace=vice-apps \
  --port=60001 \
  --gateway-name=vice \
  --gateway-namespace=qa \
  --gateway-skip-creation \
  --max-analyses=10 \
  --log-level=debug
```

## API Authentication

When `--api-auth=true` (the default), every API route except `GET /` is gated by a Keycloak-issued JWT Bearer token in the `Authorization` header. Three things must hold for a request to pass:

1. The token verifies against `--api-auth-issuer-url`.
2. The token's `azp` claim matches `--api-auth-client-id`.
3. The token either carries `--admin-role` in `realm_access.roles` (service-account path), **or** has at least one entry in its `entitlement` claim that appears in `--admin-entitlements` (human-admin path, via Keycloak group membership).

Failures of (1) or (2) yield `401 Unauthorized`; failure of (3) yields `403 Forbidden`. Either form of admission is treated identically by the rest of the API — there are no role-or-entitlement-specific routes.

Two convenience paths exist for interactive use:

- **Swagger UI login**: when `--swagger-client-id` and `--swagger-cookie-secret` are configured, browsing to `/docs/` redirects through Keycloak and stores a session cookie. Subsequent requests through the UI are authenticated automatically.
- **`vice-operator-token` CLI** for `curl` use:

  ```sh
  curl -H "Authorization: Bearer $(./bin/vice-operator-token --config ~/.vo.qa.conf)" \
       -H "Content-Type: application/json" \
       -X PUT \
       -d '{"images": ["harbor.cyverse.org/de/vice-proxy:qa"]}' \
       https://vice-api.cyverse.run/image-cache
  ```

  The `--config` JSON file holds Keycloak client-credentials settings (`keycloak_base_url`, `realm`, `client_id`, `client_secret`); the service account behind `client_id` must hold the realm role configured by `--admin-role`.

  > **Heads-up**: include `Content-Type: application/json` on `PUT`/`POST`/`DELETE` calls with a JSON body. Without it, `curl -d` defaults to `application/x-www-form-urlencoded` and Echo's binder silently form-decodes the body, so the handler sees an empty request.

## API Endpoints

All per-analysis endpoints use `:analysis-id` (the job UUID from the DE database, available as the `analysis-id` label on all K8s resources). All routes except `GET /` are subject to the auth gate described above.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Health check / greeting (unauthenticated) |
| GET | `/capacity` | Current cluster capacity and resource usage |
| GET | `/analyses` | List all VICE resources in the namespace |
| POST | `/analyses` | Receive an AnalysisBundle, transform routing, apply all resources. Returns `409 Conflict` if at capacity. |
| DELETE | `/analyses/:analysis-id` | Delete all resources for an analysis by label |
| POST | `/analyses/:analysis-id/save-and-exit` | Trigger file upload via sidecar, then delete resources |
| GET | `/analyses/:analysis-id/status` | Resource status (deployments, pods, services, routes) |
| GET | `/analyses/:analysis-id/url-ready` | Check if deployment, service, and HTTPRoute are all ready |
| POST | `/analyses/:analysis-id/download-input-files` | Trigger file-transfer sidecar to download inputs |
| POST | `/analyses/:analysis-id/save-output-files` | Trigger file-transfer sidecar to upload outputs |
| GET | `/analyses/:analysis-id/pods` | Pod info for the analysis |
| GET | `/analyses/:analysis-id/logs` | Container logs (last 5 minutes) |
| POST | `/analyses/:analysis-id/swap-route` | Swap traffic from the loading page to the analysis container once it is ready |
| GET | `/analyses/:analysis-id/permissions` | Get the permission ConfigMap for the analysis |
| PUT | `/analyses/:analysis-id/permissions` | Update the permission ConfigMap for the analysis |
| GET | `/analyses/:analysis-id/active-sessions` | List active VICE proxy sessions for the analysis |
| POST | `/analyses/:analysis-id/logout-user` | Drop a user's VICE proxy session for the analysis |
| POST | `/regenerate-network-policies` | Regenerate per-analysis NetworkPolicies (admin) |
| GET | `/image-cache` | List cached images and per-DaemonSet readiness |
| PUT | `/image-cache` | Bulk: ensure cache DaemonSets exist for the given images. `207` on partial success. |
| DELETE | `/image-cache` | Bulk: remove cache DaemonSets (idempotent) |
| POST | `/image-cache/refresh` | Bulk: roll the DaemonSets to force a re-pull (use after pushing a new image under an existing tag) |
| GET | `/image-cache/:id` | Single cached image status by slug ID |
| DELETE | `/image-cache/:id` | Remove a single cached image (idempotent) |

## Routing Transformation

The operator automatically transforms the `HTTPRoute` resources in the bundle to match the local cluster's Gateway configuration:

- **Hostname Rewriting**: Hostnames are rewritten to match the cluster's base domain (configured via `--vice-base-url`).
- **Gateway Attachment**: The `ParentRef` of each `HTTPRoute` is updated to point to the Gateway specified by `--gateway-namespace` and `--gateway-name`.
- **Loading Page**: During the initial startup phase, traffic is temporarily routed to a "loading page" service before being swapped to the actual analysis container once it is ready.

## Deploying to Kubernetes

A sample manifest is provided at `k8s/vice-operator.yml`. It includes a Deployment, Service, ServiceAccount, ClusterRole, and ClusterRoleBinding.

```bash
kubectl apply -f k8s/vice-operator.yml
```

Edit the Deployment args to match your cluster's configuration. For a cluster using a pre-existing Gateway in another namespace:

```yaml
args:
  - "--namespace=vice-apps"
  - "--gateway-namespace=qa"
  - "--gateway-name=vice"
  - "--gateway-skip-creation"
  - "--max-analyses=10"
```

The operator image is built from the same Dockerfile as app-exposer — the `vice-operator` binary is included alongside `app-exposer` in the container image. Override the entrypoint with `command: ["/vice-operator"]`.

## Configuring app-exposer to Use Operators

Add an `operators` list under the `vice` section in app-exposer's `jobservices.yml` config file. Operators are listed in priority order — the scheduler tries them sequentially and sends to the first one with available capacity.

```yaml
vice:
  operators:
    - name: "qa"
      url: "http://vice-operator.vice-apps.svc.cluster.local:60001"
    - name: "local-cluster"
      url: "https://vice-operator.local.ts.net:60001"
```

When no operators are configured, app-exposer applies resources directly to its local cluster as before (backward compatible).

### Scheduling Behavior

1. For each operator in config order, app-exposer queries `GET /capacity`.
2. If the operator has room (`runningAnalyses < maxAnalyses`), app-exposer sends `POST /analyses` with the bundle.
3. If the operator returns `409 Conflict` (race condition — filled between capacity check and launch), the next operator is tried.
4. If all operators are exhausted, the launch returns an error.

The operator that accepted the analysis is recorded in the `operator_name` column on the `jobs` table. Subsequent lifecycle operations (exit, save-and-exit) are routed to the correct operator using this column.

### Database Migration

The `operator_name` column must exist on the `jobs` table. The migration is at `de-database/migrations/000046_operator_name.up.sql`.
