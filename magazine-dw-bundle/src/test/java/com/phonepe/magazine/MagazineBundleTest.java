package com.phonepe.magazine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.phonepe.magazine.config.MagazineBundleConfiguration;
import com.phonepe.magazine.resources.MagazineResource;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.jetty.setup.ServletEnvironment;
import jakarta.servlet.Servlet;
import jakarta.servlet.ServletRegistration;
import org.junit.jupiter.api.Test;

class MagazineBundleTest {

    @Test
    void registersOnlyDashboardAndReadOnlyApiResources() {
        final TestConfiguration configuration = new TestConfiguration();
        final Environment environment = mock(Environment.class);
        final JerseyEnvironment jersey = mock(JerseyEnvironment.class);
        final ServletEnvironment servlets = mock(ServletEnvironment.class);
        final ServletRegistration.Dynamic registration = mock(ServletRegistration.Dynamic.class);
        when(environment.jersey()).thenReturn(jersey);
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

        bundle().run(configuration, environment);

        verify(jersey).register(org.mockito.ArgumentMatchers.isA(MagazineResource.class));
        verify(environment, never()).servlets();
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
