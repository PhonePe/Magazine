/**
 * Copyright (c) 2025 Original Author(s), PhonePe India Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.phonepe.magazine.server;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.github.dockerjava.api.model.Capability;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * One Aerospike container and one client, shared by every test class in the JVM.
 * <p>
 * The namespace and port are compile-time constants rather than parameters. They used to be static
 * fields assigned by the caller immediately before the holder class initialised, which meant only
 * the first caller's values were ever applied - a second call asking for a different namespace
 * silently got the first container, and which call came first depended on test ordering.
 * <p>
 * The client is owned here and closed by a JVM shutdown hook. Tests must not close it and must not
 * build their own: a client per test method leaks a connection pool and a tend thread apiece, which
 * is what the suite used to do.
 *
 * @see AerospikeContainerExtension
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AerospikeTestContainer {

    public static final String NAMESPACE = "NAMESPACE";
    private static final int SERVICE_PORT = 3000;
    private static final String IMAGE = "aerospike/aerospike-server:6.1.0.7";

    /**
     * Initialised on first access and never reassigned. Holder-class idiom, so the container starts
     * exactly once, lazily, without locking on subsequent reads.
     */
    private static final class Holder {

        private static final GenericContainer<?> CONTAINER = startContainer();
        private static final IAerospikeClient CLIENT = connect(CONTAINER);
    }

    /**
     * @return the shared container, started on first call.
     */
    public static GenericContainer<?> container() {
        return Holder.CONTAINER;
    }

    /**
     * @return the shared client. Do not close it; it outlives every test class.
     */
    public static IAerospikeClient client() {
        return Holder.CLIENT;
    }

    /**
     * @return the host port the container's service port is mapped to.
     */
    public static int mappedPort() {
        return Holder.CONTAINER.getMappedPort(SERVICE_PORT);
    }

    private static GenericContainer<?> startContainer() {
        log.info("Starting aerospike container. Docker image: {}", IMAGE);
        final GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(IMAGE))
                .withExposedPorts(SERVICE_PORT)
                .withLogConsumer(new Slf4jLogConsumer(log))
                .withEnv("NAMESPACE", NAMESPACE)
                .withEnv("SERVICE_PORT", String.valueOf(SERVICE_PORT))
                .withEnv("MEM_GB", String.valueOf(1))
                .withEnv("STORAGE_GB", String.valueOf(1))
                .withCreateContainerCmdModifier(cmd -> cmd.withCapAdd(Capability.NET_ADMIN))
                .waitingFor(new HostPortWaitStrategy())
                .withStartupTimeout(Duration.of(300, SECONDS));
        container.start();
        return container;
    }

    /**
     * The container reports ready before Aerospike accepts client connections, so poll until a
     * client actually connects rather than racing the first test. Awaitility owns the poll loop
     * and its interrupt handling, rather than a hand-rolled {@code Thread.sleep} retry.
     */
    private static IAerospikeClient connect(final GenericContainer<?> container) {
        final Host host = new Host(container.getHost(), container.getMappedPort(SERVICE_PORT));
        final AtomicReference<AerospikeException> lastFailure = new AtomicReference<>();
        final AtomicReference<IAerospikeClient> connected = new AtomicReference<>();
        try {
            Awaitility.await()
                    .atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofMillis(100))
                    .ignoreExceptions()
                    .until(() -> {
                        try {
                            final IAerospikeClient client = new AerospikeClient(new ClientPolicy(), host);
                            connected.set(client);
                            return true;
                        } catch (AerospikeException e) {
                            lastFailure.set(e);
                            return false;
                        }
                    });
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Aerospike did not become ready", lastFailure.get());
        }
        final IAerospikeClient client = connected.get();
        Runtime.getRuntime().addShutdownHook(new Thread(client::close));
        return client;
    }
}
