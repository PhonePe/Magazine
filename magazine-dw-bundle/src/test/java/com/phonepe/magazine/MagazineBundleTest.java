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

package com.phonepe.magazine;

import com.codahale.metrics.MetricRegistry;
import com.phonepe.magazine.config.MagazineBundleConfiguration;
import com.phonepe.magazine.metrics.MagazineMetrics;
import com.phonepe.magazine.metrics.StorageOperation;
import com.phonepe.magazine.resources.MagazineResource;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.jetty.setup.ServletEnvironment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.micrometer.core.instrument.Metrics;
import jakarta.servlet.Servlet;
import jakarta.servlet.ServletRegistration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class MagazineBundleTest {

    /**
     * The bundle attaches its registry to Micrometer's global registry, which outlives the test.
     * Without this a later test inherits an earlier bundle's Dropwizard bridge.
     */
    @AfterEach
    void detachGlobalRegistries() {
        Metrics.globalRegistry.getRegistries().forEach(Metrics::removeRegistry);
        Metrics.globalRegistry.clear();
    }

    @Test
    void registersOnlyDashboardAndReadOnlyApiResources() {
        final TestConfiguration configuration = new TestConfiguration();
        final Environment environment = mock(Environment.class);
        final JerseyEnvironment jersey = mock(JerseyEnvironment.class);
        final ServletEnvironment servlets = mock(ServletEnvironment.class);
        final ServletRegistration.Dynamic registration = mock(ServletRegistration.Dynamic.class);
        when(environment.jersey()).thenReturn(jersey);
        final LifecycleEnvironment lifecycle = mock(LifecycleEnvironment.class);
        when(environment.lifecycle()).thenReturn(lifecycle);
        when(environment.servlets()).thenReturn(servlets);
        when(servlets.addServlet(org.mockito.ArgumentMatchers.eq("magazineAssets"),
                org.mockito.ArgumentMatchers.any(Servlet.class))).thenReturn(registration);
        final MagazineBundle<TestConfiguration> bundle = bundle();
        bundle.run(configuration, environment);

        verify(jersey).register(org.mockito.ArgumentMatchers.isA(MagazineResource.class));
        verify(registration).addMapping("/magazineDashboard/*");
        assertEquals("bundle-test", bundle.getMagazineManager().getClientId());
    }

    @Test
    void bundleAlwaysRegistersApi() {
        final TestConfiguration configuration = new TestConfiguration();
        configuration.dashboard().setDashboardEnabled(false);
        final Environment environment = mock(Environment.class);
        final JerseyEnvironment jersey = mock(JerseyEnvironment.class);
        when(environment.jersey()).thenReturn(jersey);
        final LifecycleEnvironment lifecycle = mock(LifecycleEnvironment.class);
        when(environment.lifecycle()).thenReturn(lifecycle);

        bundle().run(configuration, environment);

        verify(jersey).register(org.mockito.ArgumentMatchers.isA(MagazineResource.class));
    }

    @Test
    void dashboardCanBeDisabledWithoutDisablingApi() {
        final TestConfiguration configuration = new TestConfiguration();
        configuration.dashboard().setDashboardEnabled(false);
        final Environment environment = mock(Environment.class);
        final JerseyEnvironment jersey = mock(JerseyEnvironment.class);
        when(environment.jersey()).thenReturn(jersey);
        final LifecycleEnvironment lifecycle = mock(LifecycleEnvironment.class);
        when(environment.lifecycle()).thenReturn(lifecycle);

        bundle().run(configuration, environment);

        verify(jersey).register(org.mockito.ArgumentMatchers.isA(MagazineResource.class));
        verify(environment, never()).servlets();
    }


    /**
     * The whole point of the bundle owning a registry: a counter recorded by magazine-core has to
     * end up in the registry Dropwizard serves on the admin port.
     */
    @Test
    void magazineMetricsReachTheDropwizardAdminRegistry() {
        final TestConfiguration configuration = new TestConfiguration();
        configuration.dashboard().setDashboardEnabled(false);
        final Environment environment = mock(Environment.class);
        final JerseyEnvironment jersey = mock(JerseyEnvironment.class);
        final MetricRegistry metrics = new MetricRegistry();
        when(environment.jersey()).thenReturn(jersey);
        final LifecycleEnvironment lifecycle = mock(LifecycleEnvironment.class);
        when(environment.lifecycle()).thenReturn(lifecycle);
        when(environment.metrics()).thenReturn(metrics);

        final MagazineBundle<TestConfiguration> bundle = bundle();
        bundle.run(configuration, environment);

        new MagazineMetrics(bundle.getMeterRegistry())
                .aerospikeCall("email-jobs", StorageOperation.CLAIM_FIRE_POINTER);

        // Micrometer tags are flattened into Dropwizard's hierarchical names.
        assertTrue(metrics.getNames().contains(
                        "magazine.aerospike.calls.magazine.email-jobs.operation.claim_fire_pointer"),
                "expected the documented dotted metric name, got " + metrics.getNames());
        // Outcomes are pre-resolved per magazine, but only the ones each meter can actually
        // report - a pooled enum would register dozens of permanently-zero series here.
        assertTrue(metrics.getNames().stream().noneMatch(name -> name.contains("dedupe.outcomes.")
                        && !(name.endsWith(".claimed") || name.endsWith(".duplicate"))),
                "dedupe outcomes must not register unreachable values: " + metrics.getNames());
    }

    /**
     * The reason storages no longer take a registry: the bundle attaches its bridge to Micrometer's
     * global registry, which is where a storage built without an explicit registry publishes. If
     * this breaks, every application that stopped passing a registry goes silently unmonitored.
     */
    @Test
    void storagesThatWereHandedNoRegistryStillReachTheAdminRegistry() {
        final TestConfiguration configuration = new TestConfiguration();
        configuration.dashboard().setDashboardEnabled(false);
        final Environment environment = mock(Environment.class);
        final JerseyEnvironment jersey = mock(JerseyEnvironment.class);
        final MetricRegistry metrics = new MetricRegistry();
        when(environment.jersey()).thenReturn(jersey);
        final LifecycleEnvironment lifecycle = mock(LifecycleEnvironment.class);
        when(environment.lifecycle()).thenReturn(lifecycle);
        when(environment.metrics()).thenReturn(metrics);

        bundle().run(configuration, environment);

        // Exactly what AerospikeStorage does when its builder was given no meterRegistry.
        new MagazineMetrics(Metrics.globalRegistry)
                .aerospikeCall("email-jobs", StorageOperation.CLAIM_FIRE_POINTER);

        assertTrue(metrics.getNames().contains(
                        "magazine.aerospike.calls.magazine.email-jobs.operation.claim_fire_pointer"),
                "global-registry metrics must reach the admin port, got " + metrics.getNames());
    }

    /**
     * Opting out must not be undone by someone else attaching a registry globally.
     */
    @Test
    void disabledBundleDoesNotAttachToTheGlobalRegistry() {
        final TestConfiguration configuration = new TestConfiguration();
        configuration.dashboard().setDashboardEnabled(false);
        configuration.dashboard().setMetricsEnabled(false);
        final Environment environment = mock(Environment.class);
        final JerseyEnvironment jersey = mock(JerseyEnvironment.class);
        final MetricRegistry metrics = new MetricRegistry();
        when(environment.jersey()).thenReturn(jersey);
        final LifecycleEnvironment lifecycle = mock(LifecycleEnvironment.class);
        when(environment.lifecycle()).thenReturn(lifecycle);
        when(environment.metrics()).thenReturn(metrics);

        bundle().run(configuration, environment);

        new MagazineMetrics(Metrics.globalRegistry)
                .aerospikeCall("email-jobs", StorageOperation.CLAIM_FIRE_POINTER);

        assertTrue(metrics.getNames().isEmpty(), "expected no meters, got " + metrics.getNames());
    }

    @Test
    void metricsCanBeDisabledSoNothingIsPublished() {
        final TestConfiguration configuration = new TestConfiguration();
        configuration.dashboard().setDashboardEnabled(false);
        configuration.dashboard().setMetricsEnabled(false);
        final Environment environment = mock(Environment.class);
        final JerseyEnvironment jersey = mock(JerseyEnvironment.class);
        final MetricRegistry metrics = new MetricRegistry();
        when(environment.jersey()).thenReturn(jersey);
        final LifecycleEnvironment lifecycle = mock(LifecycleEnvironment.class);
        when(environment.lifecycle()).thenReturn(lifecycle);
        when(environment.metrics()).thenReturn(metrics);

        final MagazineBundle<TestConfiguration> bundle = bundle();
        bundle.run(configuration, environment);

        new MagazineMetrics(bundle.getMeterRegistry())
                .aerospikeCall("email-jobs", StorageOperation.CLAIM_FIRE_POINTER);

        assertTrue(metrics.getNames().isEmpty(), "expected no meters, got " + metrics.getNames());
    }

    private static MagazineBundle<TestConfiguration> bundle() {
        return new MagazineBundle<>() {
            @Override
            protected MagazineBundleConfiguration getMagazineBundleConfiguration(
                    final TestConfiguration configuration) {
                return configuration.dashboard();
            }

            @Override
            protected String getClientId(final TestConfiguration configuration) {
                return "bundle-test";
            }
        };
    }

    private static final class TestConfiguration extends Configuration {
        private final MagazineBundleConfiguration dashboard = new MagazineBundleConfiguration();

        MagazineBundleConfiguration dashboard() {
            return dashboard;
        }
    }
}
