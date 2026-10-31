# Contributing

Thank you for helping improve the ParticleDB Java SDK.

## Build

You need Java 11 or later and Maven 3.6 or later. The SDK depends on
`ai.particledb:particledb-jdbc` from Maven Central.

```
mvn verify
```

## Test

The live-server tests skip without a server. Install the server
(`curl -fsSL https://get.particledb.ai | bash`) and point `PDB_BIN` at it;
they start their own server on free ports:

```
PDB_BIN=$(command -v particledb) mvn verify
```

Run a single class with `-Dtest=ClassName`. Before you open a pull request,
also run the conformance suite (it starts a fresh server from `PDB_BIN`, else
`particledb` on `PATH`):

```
./conformance/run_conformance.sh
```

## Code style

- Follow the style of the surrounding code: four-space indents, no tabs.
- Keep the SDK free of new runtime dependencies unless there is a strong
  reason.
- Public API needs complete Javadoc.
- Comments explain why the code does something, in plain sentences.
- Every behaviour change comes with a test that fails without it.

## Reporting issues

Open an issue at https://github.com/particledb-ai/particledb-sdk-java/issues with
the SDK version, the driver version, the server version, the Java version, a minimal
reproduction and the full stack trace. Report security problems privately as
described in [SECURITY.md](SECURITY.md).
