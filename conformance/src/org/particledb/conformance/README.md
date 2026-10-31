# conformance/src/org/particledb/conformance

`JavaSdkAdapter.java`: the Java SDK's conformance adapter (see `../../../../README.md`).
`Harness.java`, `Json.java`, `Resp.java`, `JdbcAdapter.java`: the shared harness (runner contract, case
loader, `script` interpreter, canonical value comparison, a RESP2 client, and the
JDBC reference adapter whose helpers this adapter reuses), copied
from the JDBC driver's conformance bundle, which is the reference adapter.
