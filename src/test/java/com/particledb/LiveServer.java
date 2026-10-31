package com.particledb;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test helper: a {@code --no-auth} server started from the {@code PDB_BIN}
 * binary on free ports, with a fresh data directory. {@link #binary()} is
 * {@code null} when {@code PDB_BIN} is unset, and live tests skip.
 *
 * <p>Mirrors the JDBC module's {@code PdbTestServer}: before it is used, the
 * TCP port is checked to belong to the child (lsof) and the Unix socket's
 * {@code .lock} file to name the child's PID, because a loopback client
 * connects over {@code /tmp/.s.PDB.<port>} and a foreign server holding that
 * path would otherwise answer.
 */
final class LiveServer implements AutoCloseable {

    final Process process;
    final int port;
    private final File dataDir;

    private LiveServer(Process process, int port, File dataDir) {
        this.process = process;
        this.port = port;
        this.dataDir = dataDir;
    }

    /** The executable named by {@code PDB_BIN}, or {@code null}. */
    static String binary() {
        String v = System.getenv("PDB_BIN");
        if (v == null || v.isEmpty()) return null;
        if (!new File(v).canExecute()) {
            throw new IllegalStateException("PDB_BIN is not executable: " + v);
        }
        return v;
    }

    /** Start a server from {@code PDB_BIN} (caller has checked {@link #binary()}). */
    static LiveServer start() throws IOException, InterruptedException {
        String bin = binary();
        if (bin == null) throw new IllegalStateException("PDB_BIN not set");
        int pdb = freePort();
        File dir = Files.createTempDirectory("pdb-sdk-live").toFile();
        File config = new File(dir, "config.toml");
        try (FileWriter w = new FileWriter(config)) {
            w.write("data_dir = \"" + dir.getAbsolutePath() + "\"\n\n");
            w.write("[storage]\n\n[wal]\n\n[raft]\n\n[cluster]\n\n[metrics]\n\n[security]\n\n");
            w.write("[network]\n");
            w.write("pdb_wire_port = " + pdb + "\n");
            w.write("pg_addr = \"127.0.0.1:" + freePort() + "\"\n");
            w.write("grpc_addr = \"127.0.0.1:" + freePort() + "\"\n");
            w.write("http_addr = \"127.0.0.1:" + freePort() + "\"\n");
            w.write("metrics_port = " + freePort() + "\n");
            w.write("health_port = " + freePort() + "\n");
        }
        List<String> cmd = new ArrayList<>(Arrays.asList(bin, "start", "--no-auth",
                "--config", config.getAbsolutePath(), "--data-dir", dir.getAbsolutePath()));
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        pb.redirectOutput(new File(dir, "server.log"));
        Process p = pb.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (!p.isAlive()) {
                throw new IllegalStateException("server exited during startup; see "
                        + new File(dir, "server.log"));
            }
            try (Socket s = new Socket("127.0.0.1", pdb)) {
                LiveServer srv = new LiveServer(p, pdb, dir);
                srv.assertOwnsPorts();
                return srv;
            } catch (IOException notYet) {
                Thread.sleep(100);
            }
        }
        p.destroyForcibly();
        throw new IllegalStateException("server did not listen on " + pdb + " within 30 s");
    }

    ConnectionOptions.Builder options() {
        return ConnectionOptions.builder().host("127.0.0.1").port(port);
    }

    /** Kill the server at once (no shutdown), as a crash would. */
    void kill() throws InterruptedException {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        process.destroy();
        try {
            if (!process.waitFor(10, TimeUnit.SECONDS)) process.destroyForcibly();
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
        }
        deleteRecursive(dataDir);
    }

    private void assertOwnsPorts() throws InterruptedException {
        String pid = Long.toString(process.pid());
        List<String> owners = listeningPids(port);
        if (owners != null && !owners.isEmpty() && !owners.contains(pid)
                && process.descendants().noneMatch(d -> owners.contains(Long.toString(d.pid())))) {
            process.destroyForcibly();
            throw new IllegalStateException("port " + port + " is held by " + owners
                    + ", not the server started (pid " + pid + ")");
        }
        Path lock = Paths.get("/tmp", ".s.PDB." + port + ".lock");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        String owner = null;
        while (System.nanoTime() < deadline && process.isAlive()) {
            owner = null;
            try {
                for (String line : Files.readAllLines(lock)) {
                    if (line.startsWith("pid=")) owner = line.substring(4).trim();
                }
            } catch (IOException notYet) {
                // not written yet
            }
            if (pid.equals(owner)) return;
            Thread.sleep(50);
        }
        if (owner != null) {
            process.destroyForcibly();
            throw new IllegalStateException(lock + " names pid " + owner
                    + ", not the server started (pid " + pid + ")");
        }
        // No lock file: Unix sockets are off and the driver uses TCP.
    }

    private static List<String> listeningPids(int port) {
        try {
            Process lsof = new ProcessBuilder("lsof", "-nP", "-iTCP:" + port, "-sTCP:LISTEN", "-t").start();
            List<String> pids = new ArrayList<>();
            for (String line : new String(lsof.getInputStream().readAllBytes()).split("\n")) {
                if (!line.trim().isEmpty()) pids.add(line.trim());
            }
            lsof.waitFor(5, TimeUnit.SECONDS);
            return pids;
        } catch (Exception e) {
            return null;
        }
    }

    /** A port nothing listens on (also used for connect-refused tests). */
    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void deleteRecursive(File f) {
        if (f == null || !f.exists()) return;
        File[] kids = f.listFiles();
        if (kids != null) for (File k : kids) deleteRecursive(k);
        f.delete();
    }
}
