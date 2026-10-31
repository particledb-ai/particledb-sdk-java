# conformance

The Java SDK's SDK-conformance bundle (`ParticleDB`, `PdbWireClient`, `KVOps`),
the second reference after the JDBC driver it is routed through.

```
PDB_BIN=/path/to/particledb ./run_conformance.sh [--mode default|strict] [--results FILE]
```

| entry | what it is for |
|---|---|
| `run_conformance.sh` | Fresh server (`PDB_BIN`, default `particledb` on `PATH`); builds the SDK from source (the JDBC driver from Maven Central); compiles the adapter with the shared harness (`src/`); runs it with no JVM module flags and without Apache Arrow Java on the classpath; validates |
| `src/org/particledb/conformance/JavaSdkAdapter.java` | The adapter: every case through the SDK's own API; isolation levels and savepoints through `ParticleDB.getConnection()` (the SDK's documented escape hatch); a surface the SDK does not offer is a justified skip |
| `cases/`, `lib/`, `SPEC.md` | The shared SDK conformance suite, identical in every ParticleDB SDK repository; change every copy together |
