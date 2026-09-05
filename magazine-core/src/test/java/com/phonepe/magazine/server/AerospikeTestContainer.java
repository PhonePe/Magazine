package com.phonepe.magazine.server;

import static java.time.temporal.ChronoUnit.SECONDS;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.github.dockerjava.api.model.Capability;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class AerospikeTestContainer {

    private static String namespace;
    private static int port;
    private static GenericContainer<?> aerospike;


    private static class SingletonHolder {
        public static int loadClass = 0;

        static {
            String aerospikeDockerImage = "aerospike/aerospike-server:6.1.0.7";
            log.info("Starting aerospike container. Docker image: {}", aerospikeDockerImage);

            GenericContainer<?> aerospikeContainer =
                    new GenericContainer<>(DockerImageName.parse(aerospikeDockerImage))
                            .withExposedPorts(port)
                            .withLogConsumer(new Slf4jLogConsumer(log))
                            .withEnv("NAMESPACE", namespace)
                            .withEnv("SERVICE_PORT", String.valueOf(port))
                            .withEnv("MEM_GB", String.valueOf(1))
                            .withEnv("STORAGE_GB", String.valueOf(1))
                            .withCreateContainerCmdModifier(cmd -> cmd.withCapAdd(Capability.NET_ADMIN))
                            .waitingFor(new HostPortWaitStrategy())
                            .withStartupTimeout(Duration.of(300, SECONDS));

            aerospikeContainer.start();
            awaitAerospike(aerospikeContainer);
            aerospike = aerospikeContainer;
        }
    }

    private static void awaitAerospike(final GenericContainer<?> container) {
        final long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        AerospikeException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try (AerospikeClient ignored = new AerospikeClient(
                    container.getHost(), container.getMappedPort(port))) {
                return;
            } catch (AerospikeException e) {
                lastFailure = e;
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for Aerospike", interruptedException);
                }
            }
        }
        throw new IllegalStateException("Aerospike did not become ready", lastFailure);
    }

    /**
     * Method to start aerospike container for test cases.
     */
    public static GenericContainer<?> initServerForTesting(final String namespace, final int port) {
        // To make sure we load class which will start the container.
        AerospikeTestContainer.namespace = namespace;
        AerospikeTestContainer.port = port;
        SingletonHolder.loadClass = 1;
        return aerospike;
    }


}
