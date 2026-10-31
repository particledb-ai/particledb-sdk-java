/**
 * ParticleDB Java SDK: {@link com.particledb.ParticleDB} connects over the
 * native JDBC driver ({@code ai.particledb:particledb-jdbc}).
 *
 * <p>Requires Java 11 or later. No JVM flags are needed on any JDK: the
 * driver decodes every result type (numeric, date, time, timestamp, arrays,
 * ...) with its own pure-Java Arrow IPC reader, and Apache Arrow Java is not a
 * dependency. Older releases needed
 * {@code --add-opens=java.base/java.nio=ALL-UNNAMED} on JDK 16 and later;
 * passing it now is harmless.
 *
 * <p>{@link com.particledb.Config#resultDecoder(String)} selects the decoder:
 * {@code auto} (default) or {@code builtin}, the built-in reader, or
 * {@code arrow}, Apache Arrow Java, which the application then adds itself.
 */
package com.particledb;
