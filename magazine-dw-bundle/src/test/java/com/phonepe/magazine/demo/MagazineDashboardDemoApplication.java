package com.phonepe.magazine.demo;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.MagazineBundle;
import com.phonepe.magazine.config.MagazineBundleConfiguration;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Test-only application for viewing the dashboard without Aerospike. */
public final class MagazineDashboardDemoApplication extends Application<MagazineDashboardDemoConfiguration> {

    private final MagazineBundle<MagazineDashboardDemoConfiguration> magazineBundle = new MagazineBundle<>() {
        @Override
        protected MagazineBundleConfiguration getMagazineBundleConfiguration(
                final MagazineDashboardDemoConfiguration configuration) {
            return configuration.getMagazineBundle();
        }

        @Override
        protected String getClientId(final MagazineDashboardDemoConfiguration configuration) {
            return "dashboard-demo";
        }

    };

    public static void main(final String[] args) throws Exception {
        new MagazineDashboardDemoApplication().run(args);
    }

    @Override
    public String getName() {
        return "magazine-dashboard-demo";
    }

    @Override
    public void initialize(final Bootstrap<MagazineDashboardDemoConfiguration> bootstrap) {
        bootstrap.addBundle(magazineBundle);
    }

    @Override
    public void run(final MagazineDashboardDemoConfiguration configuration, final Environment environment) {
        // Demo only: grant the peek role unconditionally so the dashboard can show payloads.
        // A real deployment registers proper authentication and grants the role selectively.
        environment.jersey().register(
                new com.phonepe.magazine.testsupport.GrantRoleFilter(
                        com.phonepe.magazine.resources.MagazineResource.PEEK_ROLE));
        final Magazine<String> emailJobs = magazine(
                "email-jobs",
                Map.of(
                        "SHARD_0", new MetaData(12, 32, 12, 36),
                        "SHARD_1", new MetaData(8, 27, 9, 31)),
                Set.of(
                        new MagazineData<>("welcome-email:user-1042", 35, 0, "email-jobs"),
                        new MagazineData<>("receipt-email:order-8821", 36, 0, "email-jobs"),
                        new MagazineData<>("password-reset:user-991", 31, 1, "email-jobs")));
        final Magazine<String> paymentRetries = magazine(
                "payment-retries",
                Map.of(
                        "SHARD_0", new MetaData(41, 48, 43, 51),
                        "SHARD_1", new MetaData(38, 44, 39, 47)),
                Set.of(
                        new MagazineData<>("payment:PAY-202601", 50, 0, "payment-retries"),
                        new MagazineData<>("payment:PAY-202602", 51, 0, "payment-retries"),
                        new MagazineData<>("payment:PAY-202603", 47, 1, "payment-retries")));
        magazineBundle.getMagazineManager().refresh(java.util.List.of(emailJobs, paymentRetries));
    }

    @SuppressWarnings("unchecked")
    private static Magazine<String> magazine(final String identifier,
            final Map<String, MetaData> metadata,
            final Set<MagazineData<String>> records) {
        final Magazine<String> magazine = mock(Magazine.class);
        when(magazine.getMagazineIdentifier()).thenReturn(identifier);
        when(magazine.getShards()).thenReturn(metadata.size());
        when(magazine.getMetaData()).thenReturn(new LinkedHashMap<>(metadata));
        when(magazine.peek(anyMap())).thenAnswer(invocation -> {
            final Map<Integer, Set<Long>> requested = invocation.getArgument(0);
            return records.stream()
                    .filter(record -> requested.getOrDefault(record.getShard(), Set.of())
                            .contains(record.getFirePointer()))
                    .collect(java.util.stream.Collectors.toSet());
        });
        return magazine;
    }
}
