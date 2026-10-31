package io.particledb.sdk;

import io.grpc.StatusRuntimeException;

/**
 * Exception thrown by {@link GrpcClient} when a gRPC call fails.
 *
 * Wraps the underlying {@link StatusRuntimeException} with additional
 * context about which operation was attempted.
 */
public class GrpcClientException extends RuntimeException {

    /**
     * Create a new GrpcClientException with a message.
     *
     * @param message description of the failed operation
     */
    public GrpcClientException(String message) {
        super(message);
    }

    /**
     * Create a new GrpcClientException wrapping a cause.
     *
     * @param message description of the failed operation
     * @param cause   the underlying exception
     */
    public GrpcClientException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Create a new GrpcClientException from a cause only.
     *
     * @param cause the underlying exception
     */
    public GrpcClientException(Throwable cause) {
        super(cause);
    }
}
