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

package com.phonepe.magazine.demo;

import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.MagazineBundle;
import com.phonepe.magazine.config.MagazineBundleConfiguration;
import com.phonepe.magazine.entity.FireCheckpoint;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineExceptions;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test-only application for viewing the dashboard without Aerospike.
 */
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
                        new MagazineData<>("password-reset:user-991", 31, 1, "email-jobs")),
                // SHARD_1 only starts recording partway through, as a shard does when it is first
                // loaded to, so the oldest windows show one shard reporting rather than two.
                Map.of(
                        "SHARD_0", checkpoints(34, 6, 8),
                        "SHARD_1", checkpoints(29, 5, 5)));
        final Magazine<String> paymentRetries = magazine(
                "payment-retries",
                Map.of(
                        "SHARD_0", new MetaData(41, 48, 43, 51),
                        "SHARD_1", new MetaData(38, 44, 39, 47)),
                Set.of(
                        new MagazineData<>("payment:PAY-202601", 50, 0, "payment-retries"),
                        new MagazineData<>("payment:PAY-202602", 51, 0, "payment-retries"),
                        new MagazineData<>("payment:PAY-202603", 47, 1, "payment-retries")),
                // Fire history left off, so the dashboard shows the disabled state rather than an error.
                null);
        magazineBundle.getMagazineManager().replaceAll(java.util.List.of(emailJobs, paymentRetries));
    }

    /**
     * A descending run of checkpoints on five-minute windows, newest first, as the recorder would
     * have written them.
     */
    private static java.util.List<FireCheckpoint> checkpoints(final long newest, final long perWindow,
                                                              final int windows) {
        final java.util.List<FireCheckpoint> checkpoints = new java.util.ArrayList<>(windows);
        for (int window = 0; window < windows; window++) {
            checkpoints.add(new FireCheckpoint(newest - (long) window * perWindow,
                    Instant.now().minus(Duration.ofMinutes(5L * window + 2))));
        }
        return checkpoints;
    }

    @SuppressWarnings("unchecked")
    private static Magazine<String> magazine(final String identifier,
                                             final Map<String, MetaData> metadata,
                                             final Set<MagazineData<String>> records,
                                             final Map<String, java.util.List<FireCheckpoint>> checkpoints) {
        final Magazine<String> magazine = mock(Magazine.class);
        when(magazine.getMagazineIdentifier()).thenReturn(identifier);
        when(magazine.getShards()).thenReturn(metadata.size());
        when(magazine.getMetaData()).thenReturn(new LinkedHashMap<>(metadata));
        if (checkpoints == null) {
            when(magazine.fireHistory()).thenThrow(
                    MagazineExceptions.of(ErrorCode.NOT_ENABLED, "Fire history is not enabled for " + identifier));
        } else {
            when(magazine.fireHistory()).thenReturn(new LinkedHashMap<>(checkpoints));
        }
        when(magazine.peek(anyMap())).thenAnswer(invocation -> {
            final Map<Integer, Set<Long>> requested = invocation.getArgument(0);
            return records.stream()
                    .filter(magazineData -> requested.getOrDefault(magazineData.getShard(), Set.of())
                            .contains(magazineData.getFirePointer()))
                    .collect(java.util.stream.Collectors.toSet());
        });
        return magazine;
    }
}
