# River Pro main docs examples

This folder contains DB-backed tests for every example block found in the uploaded River Pro main package docs (`prodocs-full/index.md`).

The uploaded docs contained 17 example blocks:

1. BatchWorker
2. ClientSetup
3. DurablePeriodicJob
4. EphemeralJob
5. EphemeralQueue
6. GlobalConcurrencyLimiting
7. PerQueueRetention
8. WorkflowDependencyOutput
9. WorkflowTaskSignalData
10. WorkflowWaitMixedTermsAndRawCEL
11. WorkflowWaitRawCEL
12. WorkflowWaitResult_timeoutVsSignal
13. WorkflowWaitSignalQuorum
14. WorkflowWaitTimerFallback
15. ManualReview
16. WorkflowAuditPagination
17. LatestEvidenceSignal

Run with:

```sh
TEST_DATABASE_URL='postgres://oai@localhost:55432/river_test?sslmode=disable' \
  go test -vet=off -count=1 -p=1 ./riverpro/examples
```

The tests intentionally use the same River test-schema style as the rest of the repository so each example runs against a migrated `main` + `pro` schema.
