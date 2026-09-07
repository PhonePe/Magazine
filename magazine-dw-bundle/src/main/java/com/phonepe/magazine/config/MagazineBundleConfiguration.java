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

package com.phonepe.magazine.config;

import lombok.Data;

@Data
public final class MagazineBundleConfiguration {

    private boolean dashboardEnabled = true;

    /**
     * Seconds to cache a magazine's shard metadata for; {@code 0} disables caching.
     * <p>
     * Rendering the dashboard costs one batch read per magazine, fanning out to every shard, so an
     * open dashboard - or anything pointed at {@code /metadata} on a timer - issues
     * {@code magazines x shards} key reads per refresh against the same cluster serving production
     * traffic.
     */
    private int metadataCacheSeconds = 5;

    /**
     * Whether the bundle publishes Magazine's metrics through the application's metric registry,
     * making them visible on the Dropwizard admin port alongside everything else the service
     * reports.
     */
    private boolean metricsEnabled = true;
}
