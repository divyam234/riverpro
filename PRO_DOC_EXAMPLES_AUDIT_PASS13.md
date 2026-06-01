# Pro docs examples audit pass 13

Source docs audited: `/mnt/data/prodocs-full/index.md`.

The main River Pro package docs contained 17 example blocks, not 14 in the uploaded copy. All 17 are represented in `riverpro/examples/pro_docs_examples_test.go` and are executed as subtests under `TestProMainDocsExamples`.

Covered examples:

- BatchWorker
- ClientSetup
- DurablePeriodicJob
- EphemeralJob
- EphemeralQueue
- GlobalConcurrencyLimiting
- PerQueueRetention
- WorkflowDependencyOutput
- WorkflowTaskSignalData
- WorkflowWaitMixedTermsAndRawCEL
- WorkflowWaitRawCEL
- WorkflowWaitResult_timeoutVsSignal
- WorkflowWaitSignalQuorum
- WorkflowWaitTimerFallback
- ManualReview
- WorkflowAuditPagination
- LatestEvidenceSignal

Verification environment:

- Go 1.25.7 from uploaded toolchain
- PostgreSQL 18.3 from uploaded tarball
- Offline module cache from uploaded Go module cache
- `GOPROXY=off`
- `TEST_DATABASE_URL=postgres://oai@localhost:55432/river_test?sslmode=disable`

Commands passed:

```sh
go test -vet=off -timeout=180s -count=1 -p=1 -v ./riverpro/examples
```

```sh
go test -vet=off -timeout=240s -count=1 -p=1 ./riverpro/...
```

Implementation fix found by examples:

- `riverworkflow.WaitFromMetadata` now treats metadata without a `phase` field as a stored `WaitSpec` and converts it through `WaitSpec.Status()`. This preserves signal/timer inputs for wait examples that inspect task wait metadata immediately after prepare/insert.

Note:

- I attempted a race run for `./riverpro/examples`, but the sandbox command timed out/reset before producing a result. I am not marking race as passed for pass 13. Normal DB-backed examples and full Pro package tests passed.
