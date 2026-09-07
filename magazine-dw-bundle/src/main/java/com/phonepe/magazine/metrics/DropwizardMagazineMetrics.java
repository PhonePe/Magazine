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

package com.phonepe.magazine.metrics;

import io.dropwizard.core.setup.Environment;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Publishes Magazine's Micrometer meters through the application's Dropwizard metric registry, so
 * they appear on the admin port next to everything else the service reports and are picked up by
 * whatever already scrapes it.
 * <p>
 * Micrometer's dimensional tags have to be flattened into Dropwizard's hierarchical names, so
 * {@code magazine.fire.latency} tagged {@code magazine=email-jobs} becomes
 * {@code magazine.fire.latency.magazine.email-jobs}. Tags survive as name segments; nothing is
 * lost, but a Grafana query has to match on the flattened name rather than on a label.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DropwizardMagazineMetrics {

    public static MeterRegistry bridgedTo(final Environment environment) {
        final DropwizardConfig config = new DropwizardConfig() {
            @Override
            public String prefix() {
                return "magazine";
            }

            @Override
            public String get(final String key) {
                return null;
            }
        };
        final MeterRegistry registry = new DropwizardMeterRegistry(config, environment.metrics(),
                HierarchicalNameMapper.DEFAULT, Clock.SYSTEM) {
            @Override
            protected Double nullGaugeValue() {
                return null;
            }
        };
        // Dropwizard's default convention camel-cases meter names, turning magazine.aerospike.calls
        // into magazineAerospikeCalls. Keeping dots preserves the documented metric names, so a
        // dashboard query matches what the docs say.
        registry.config().namingConvention(NamingConvention.dot);
        return registry;
    }
}
