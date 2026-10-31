package io.particledb.sdk;

import com.particledb.v1.BatchExecuteRequest;
import com.particledb.v1.BatchExecuteResponse;
import com.particledb.v1.ExecuteSqlRequest;
import com.particledb.v1.ExecuteSqlResponse;
import com.particledb.v1.HealthRequest;
import com.particledb.v1.HealthResponse;
import com.particledb.v1.ParticleDBGrpc;
import com.spanner.query.QueryRequest;
import com.spanner.query.QueryResponse;
import com.spanner.query.QueryServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * gRPC client for ParticleDB.
 *
 * Provides typed methods for SQL execution, batch operations, streaming
 * queries, and health checks over the ParticleDB gRPC API.
 *
 * <p>The client wraps two gRPC services:
 * <ul>
 *   <li>{@code ParticleDB} service ({@code particledb.proto}) -- SQL execution, batch, CDC, health</li>
 *   <li>{@code QueryService} service ({@code query.proto}) -- query with stats, streaming, DML execution</li>
 * </ul>
 *
 * <pre>{@code
 * try (GrpcClient client = new GrpcClient("localhost", 9090)) {
 *     ExecuteSqlResponse response = client.executeSQL("SELECT * FROM users");
 *     for (Row row : response.getRowsList()) {
 *         System.out.println(row);
 *     }
 * }
 * }</pre>
 */
public class GrpcClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final ParticleDBGrpc.ParticleDBBlockingStub particleDbStub;
    private final QueryServiceGrpc.QueryServiceBlockingStub queryStub;

    /**
     * Create a new gRPC client connecting to the given host and port.
     *
     * <p>Uses plaintext (insecure) transport by default. For TLS, use
     * {@link #GrpcClient(ManagedChannel)}.
     *
     * @param host the server hostname
     * @param port the server gRPC port
     */
    public GrpcClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build());
    }

    /**
     * Create a new gRPC client using a pre-configured channel.
     *
     * <p>This constructor allows full control over channel configuration
     * (TLS, interceptors, load balancing, etc.).
     *
     * @param channel the managed channel to use
     */
    public GrpcClient(ManagedChannel channel) {
        this.channel = channel;
        this.particleDbStub = ParticleDBGrpc.newBlockingStub(channel);
        this.queryStub = QueryServiceGrpc.newBlockingStub(channel);
    }

    // -----------------------------------------------------------------------
    // ParticleDB service (particledb.proto)
    // -----------------------------------------------------------------------

    /**
     * Execute a single SQL statement and return the full response.
     *
     * <p>Works for both queries (SELECT) and DML (INSERT, UPDATE, DELETE).
     * For SELECT statements, the response contains columns and rows.
     * For DML statements, {@code rows_affected} is populated.
     *
     * @param sql the SQL statement to execute
     * @return the execution response
     * @throws GrpcClientException if the RPC fails
     */
    public ExecuteSqlResponse executeSQL(String sql) {
        try {
            ExecuteSqlRequest request = ExecuteSqlRequest.newBuilder()
                    .setSql(sql)
                    .build();
            return particleDbStub.executeSQL(request);
        } catch (StatusRuntimeException e) {
            throw new GrpcClientException("executeSQL failed", e);
        }
    }

    /**
     * Execute a query via the QueryService, returning a response with
     * result set metadata, rows, and execution statistics.
     *
     * <p>Prefer this method over {@link #executeSQL(String)} when you need
     * query statistics (rows scanned, execution time) or pagination.
     *
     * @param sql the SQL query to execute
     * @return the query response with result set and stats
     * @throws GrpcClientException if the RPC fails
     */
    public QueryResponse query(String sql) {
        try {
            QueryRequest request = QueryRequest.newBuilder()
                    .setSql(sql)
                    .build();
            return queryStub.query(request);
        } catch (StatusRuntimeException e) {
            throw new GrpcClientException("query failed", e);
        }
    }

    /**
     * Execute multiple SQL statements in a single batch RPC.
     *
     * <p>Statements are executed sequentially on the server. Each
     * statement produces its own {@link ExecuteSqlResponse} in the
     * returned list.
     *
     * @param sqls the list of SQL statements to execute
     * @return the batch response containing one result per statement
     * @throws GrpcClientException if the RPC fails
     */
    public BatchExecuteResponse batchExecute(List<String> sqls) {
        try {
            BatchExecuteRequest request = BatchExecuteRequest.newBuilder()
                    .addAllStatements(sqls)
                    .build();
            return particleDbStub.batchExecute(request);
        } catch (StatusRuntimeException e) {
            throw new GrpcClientException("batchExecute failed", e);
        }
    }

    /**
     * Execute a streaming query that returns results incrementally.
     *
     * <p>Uses the {@code QueryService.QueryStream} server-streaming RPC.
     * The returned iterator yields {@link QueryResponse} chunks as they
     * arrive from the server. Each chunk may contain a partial result set.
     *
     * <p>The iterator must be fully consumed or the underlying RPC will
     * remain open. Use in a try-with-resources or ensure complete
     * iteration.
     *
     * @param sql the SQL query to stream
     * @return an iterator over response chunks
     * @throws GrpcClientException if the initial RPC setup fails
     */
    public Iterator<QueryResponse> streamQuery(String sql) {
        try {
            QueryRequest request = QueryRequest.newBuilder()
                    .setSql(sql)
                    .build();
            return queryStub.queryStream(request);
        } catch (StatusRuntimeException e) {
            throw new GrpcClientException("streamQuery failed", e);
        }
    }

    /**
     * Collect all results from a streaming query into a list.
     *
     * <p>Convenience method that fully drains the stream. For very large
     * result sets, prefer {@link #streamQuery(String)} and process chunks
     * incrementally.
     *
     * @param sql the SQL query to stream
     * @return all response chunks collected into a list
     * @throws GrpcClientException if the RPC fails
     */
    public List<QueryResponse> streamQueryAll(String sql) {
        List<QueryResponse> results = new ArrayList<>();
        Iterator<QueryResponse> it = streamQuery(sql);
        while (it.hasNext()) {
            results.add(it.next());
        }
        return results;
    }

    // -----------------------------------------------------------------------
    // Health
    // -----------------------------------------------------------------------

    /**
     * Check server health.
     *
     * @return the health response containing server status and version
     * @throws GrpcClientException if the RPC fails
     */
    public HealthResponse health() {
        try {
            return particleDbStub.health(HealthRequest.getDefaultInstance());
        } catch (StatusRuntimeException e) {
            throw new GrpcClientException("health check failed", e);
        }
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /**
     * Shut down the gRPC channel gracefully.
     *
     * <p>Waits up to 5 seconds for in-flight RPCs to complete before
     * forcing shutdown.
     */
    @Override
    public void close() {
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Check whether the underlying channel has been shut down.
     *
     * @return true if the channel is shut down
     */
    public boolean isShutdown() {
        return channel.isShutdown();
    }

    /**
     * Check whether the underlying channel has terminated.
     *
     * @return true if the channel has terminated
     */
    public boolean isTerminated() {
        return channel.isTerminated();
    }
}
