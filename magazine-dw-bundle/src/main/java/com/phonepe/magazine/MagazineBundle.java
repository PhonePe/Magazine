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

import com.phonepe.magazine.config.MagazineBundleConfiguration;
import com.phonepe.magazine.resources.MagazineResource;
import com.phonepe.magazine.service.MagazineService;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.core.ConfiguredBundle;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import java.util.Objects;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class MagazineBundle<T extends Configuration> implements ConfiguredBundle<T> {

    private MagazineManager magazineManager;

    @Override
    public void initialize(final Bootstrap<?> bootstrap) {
        // Nothing to initialize before application configuration is available.
    }

    @Override
    public void run(final T configuration, final Environment environment) {
        final MagazineBundleConfiguration bundleConfiguration = Objects.requireNonNull(
                getMagazineBundleConfiguration(configuration), "magazineBundle configuration");
        this.magazineManager = new MagazineManager(
                Objects.requireNonNull(getClientId(configuration), "clientId"));
        final MagazineService service = new MagazineService(magazineManager, environment.getObjectMapper());

        environment.jersey().register(RolesAllowedDynamicFeature.class);
        environment.jersey().register(new MagazineResource(service));
        log.info("Magazine peek requires role '{}'. Register your own authentication and grant "
                + "that role; without a SecurityContext peek stays closed.", MagazineResource.PEEK_ROLE);

        if (bundleConfiguration.isDashboardEnabled()) {
            new AssetsBundle(
                    "/magazineAssets/",
                    "/magazineDashboard",
                    "magazineIndex.html",
                    "magazineAssets")
                    .run(configuration, environment);
        }
    }


    protected abstract MagazineBundleConfiguration getMagazineBundleConfiguration(T configuration);

    protected abstract String getClientId(T configuration);
}
