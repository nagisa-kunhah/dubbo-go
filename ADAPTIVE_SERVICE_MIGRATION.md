# Adaptive Service Migration Validation

## Checkouts

- Extensions: `/home/nagisa/dubbo-go-extensions`, branch
  `feat/migrate-server-throttling-client`.
  PR #26 was already squash-merged with the same provider implementation.
  The consumer migration includes actual-method metrics keys, synchronized
  singleton initialization, and direct Triple response trailer handling.
- Core: `/home/nagisa/dubbo-go-adaptivesvc-extensions`, branch
  `feat/adaptivesvc-extensions`, based on official develop `db30c00d`.
  The original `/home/nagisa/dubbo-go` experiment checkout and its stashes were
  not changed.
- Validation used Go 1.26.2; both modules retain `go 1.25.0`.

## Replacement

Core now imports the extensions provider filter, adaptive cluster and P2C
implementation through both `imports` and the legacy aggregate packages.
The duplicate core implementations and their private implementation tests were
removed. Their migrated tests run in extensions. The shared capacity store is
`github.com/apache/dubbo-go-extensions/cluster/metrics`.

Existing configuration keys, consumer/provider configuration fields and core
options remain available. Core verbose initialization calls the extension's
atomic `SetVerbose` API. The public adaptive error helpers in `cluster/utils`
now refer to the extension's errors.

This preserves configuration-based use, but it does not preserve source imports
of the removed implementation packages. Applications importing those packages
directly must use the corresponding `github.com/apache/dubbo-go-extensions/...`
paths, including the metrics store and limiter API.

## Issues Found And Fixed

1. Extensions P2C still panicked on invalid capacity types, while current core
   probes providers with nil or invalid metrics. Extensions now matches current
   core, with regression cases for either candidate.
2. Core `WithServerAdaptiveService` set the provider flag without propagating
   it to the service's filter switch. Service initialization now propagates it.
3. Enabling both legacy and typed provider options could install `padasvc`
   twice. Filter names are now merged with duplicate suppression.
4. The extension provider response hook depended on the response echoing the
   request's enable attachment. Triple's ServiceInfo invoker does not echo it,
   so capacity was never released or reported. The hook now uses the request
   enable flag, while retaining the response fallback for compatibility.
5. The current extensions cluster captures Triple response trailers itself,
   preserving caller-provided targets. The earlier core change that copied all
   response metadata into RPCResult attachments is no longer needed and has
   been removed together with its dedicated test. Triple transport code is
   unchanged by this migration.

## Regression Coverage

- Registry type checks prove `padasvc`, `adaptiveService` and `p2c` resolve to
  extensions implementations.
- Provider filter -> response attachments -> consumer metrics -> P2C selection.
- 80 concurrent calls against the initial capacity of 50: 50 enter the handler,
  30 are rejected; admitted calls complete and release capacity.
- Legacy provider options, typed extension options, raw extension configuration,
  disabled configuration, verbose validation and mixed option deduplication.
- Legacy global provider verbose initialization, including validation-only mode.
- Real loopback Triple calls using legacy consumer configuration and the new
  extension client option; both metadata-target and ordinary calls update the
  same extension metrics store.
- Preservation of explicit caller metadata targets in real Triple calls.

## Current Verification (2026-11-01)

Updated core dependencies to match current extensions requirements:
`github.com/dubbogo/gost v1.15.0` and `go.uber.org/atomic v1.12.0`, then ran
`go mod tidy`.

After removing the core Triple metadata change:

```sh
go test -race . ./server ./imports -run TestAdaptiveService -count=1 -timeout=90s
```

Passed, including registry ownership, capacity round-trip, P2C selection,
concurrent admission, legacy/typed options and real Triple calls with and
without explicit response metadata targets.

A broader race run also passed root, imports, client, cluster, Triple and all
extensions packages against this core checkout. The full server package failed
`TestCfgAPI_Call` and `TestIDLServiceServesGenericInvocation`: global logger
replacement in instance initialization races with background Triple logging.
This run is not all green; no unchanged-core baseline was rerun for these races.
The broad run used a temporary modfile with the same required version updates.

Logs:

- `/tmp/adaptivesvc-review.YrZxDr/focused.log`
- `/tmp/adaptivesvc-review.YrZxDr/tests.log`

## Earlier Verification

The following results describe the original migration validation, before the
current cleanup. The temporary workspace and logs may no longer exist.

Extensions with its pinned core dependency:

```sh
cd /home/nagisa/dubbo-go-extensions
go test -count=1 ./...
```

Passed. Extensions against the modified latest core:

```sh
GOWORK=/tmp/adaptivesvc-migration.go.work go test -race ./...
```

Passed. The temporary workspace lists both checkouts as `use` modules.

Core focused migration verification:

```sh
cd /home/nagisa/dubbo-go-adaptivesvc-extensions
go test -race . ./server ./imports ./protocol/triple \
  -run 'TestAdaptiveService|TestTripleInvokerUnaryResponseAttachments' -count=1
```

Passed. The client, server and cluster package suites also passed.

The full core suite was run before and after replacement. Before replacement,
three ZooKeeper watcher tests and two etcd client tests failed. The final
uncached run (`go test -count=1 -timeout 120s ./...`) after replacement failed
only tests that had already failed in the baseline:

- `TestGetPropertiesCacheUpdatedByWatch`
- `TestRestartCallBackRestoresBusinessListener`
- `TestRestartCallBackRestoresBusinessListenerWhenCacheDisabled`
- `TestNewServiceDiscoveryClient` in `remoting/etcdv3`

No new failing tests were observed; the full core suite is not all green.
These are tests exercising local ZooKeeper/etcd services. Their failures also
occurred in the unchanged baseline; their causes were not repaired as part of
this migration. The etcd failures varied between runs.

Logs:

- `/tmp/dubbo-go-before-migration-test.log`
- `/tmp/dubbo-go-after-migration-final.log`
- `/tmp/extensions-latest-core-race-test.log`
- `/tmp/adaptivesvc-core-race-test.log`

## Local Dependency Wiring

Core's `go.mod` has a local replacement:

```go
replace github.com/apache/dubbo-go-extensions => ../dubbo-go-extensions
```

This is intentional for validating the local client migration. The
required extensions revision currently identifies main's provider-only commit;
it cannot supply the new consumer packages without the local replacement.
Before publishing the core migration, first publish the extensions changes,
update the required revision to that commit and remove the local replacement.
No changes have been committed or pushed by this migration session.
