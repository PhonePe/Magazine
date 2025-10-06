package com.phonepe.magazine.server;

import static java.time.temporal.ChronoUnit.SECONDS;

import com.github.dockerjava.api.model.Capability;
import io.appform.testcontainers.aerospike.AerospikeContainerConfiguration;
import io.appform.testcontainers.aerospike.AerospikeWaitStrategy;

import io.appform.testcontainers.commons.ContainerUtils;

import java.time.Duration;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

@Slf4j
public class AerospikeTestContainer {

    private static AerospikeContainerConfiguration aerospikeContainerConfig;
    private static AerospikeWaitStrategy aerospikeWaitStrategy;
    private static GenericContainer aerospike;


    private static class SingletonHolder {
        public static int loadClass = 0;

        static {
            String aerospikeDockerImage = "aerospike/aerospike-server:6.1.0.7";
            log.info("Starting aerospike container. Docker image: {}", aerospikeDockerImage);

            WaitStrategy waitStrategy = new WaitAllStrategy()
                    .withStrategy(aerospikeWaitStrategy)
                    .withStrategy(new HostPortWaitStrategy())
                    .withStartupTimeout(Duration.of(300, SECONDS));

            GenericContainer aerospikeContainer =
                    new GenericContainer<>(aerospikeDockerImage)
                            .withExposedPorts(aerospikeContainerConfig.getPort())
                            .withLogConsumer(ContainerUtils.containerLogsConsumer(log))
                            .withEnv("NAMESPACE", aerospikeContainerConfig.getNamespace())
                            .withEnv("SERVICE_PORT", String.valueOf(aerospikeContainerConfig.getPort()))
                            .withEnv("MEM_GB", String.valueOf(1))
                            .withEnv("STORAGE_GB", String.valueOf(1))
                            .withCreateContainerCmdModifier(cmd -> cmd.withCapAdd(Capability.NET_ADMIN))
                            .waitingFor(waitStrategy)
                            .withStartupTimeout(aerospikeContainerConfig.getTimeoutDuration());

            aerospikeContainer.start();
            aerospike = aerospikeContainer;
        }
    }

    /**
     * Method to start aerospike container for test cases.
     */
    public static GenericContainer initServerForTesting(AerospikeContainerConfiguration config,
                                                        AerospikeWaitStrategy waitStrategy) {
        // To make sure we load class which will start the container.
        aerospikeContainerConfig = config;
        aerospikeWaitStrategy = waitStrategy;
        SingletonHolder.loadClass = 1;
        return aerospike;
    }


}
