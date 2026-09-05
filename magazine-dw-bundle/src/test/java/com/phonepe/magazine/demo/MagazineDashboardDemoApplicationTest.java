package com.phonepe.magazine.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class MagazineDashboardDemoApplicationTest {

    @RegisterExtension
    static final DropwizardAppExtension<MagazineDashboardDemoConfiguration> APPLICATION =
            new DropwizardAppExtension<>(
                    MagazineDashboardDemoApplication.class,
                    ResourceHelpers.resourceFilePath("dashboard-demo.yml"),
                    ConfigOverride.randomPorts());

    @Test
    void servesDashboardAssetsAndReadOnlyApi() {
        final String dashboard = APPLICATION.client()
                .target("http://localhost:" + APPLICATION.getLocalPort() + "/magazineDashboard/")
                .request()
                .get(String.class);
        assertTrue(dashboard.contains("Magazine Observatory"));

        final String magazines = APPLICATION.client()
                .target("http://localhost:" + APPLICATION.getLocalPort() + "/magazine/v1/magazines")
                .request()
                .get(String.class);
        assertTrue(magazines.contains("email-jobs"));
        assertTrue(magazines.contains("payment-retries"));

        final String peek = APPLICATION.client()
                .target("http://localhost:" + APPLICATION.getLocalPort()
                        + "/magazine/v1/magazines/email-jobs/peek")
                .request()
                .post(Entity.json("{\"pointers\":{\"0\":[\"35\"]}}"), String.class);
        assertTrue(peek.contains("welcome-email:user-1042"));

        try (Response response = APPLICATION.client()
                .target("http://localhost:" + APPLICATION.getLocalPort()
                        + "/magazine/v1/magazines/email-jobs/fire")
                .request()
                .post(Entity.json("{}"))) {
            assertEquals(404, response.getStatus());
        }
    }
}
