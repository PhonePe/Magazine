/**
 * Copyright (c) 2025 Original Author(s), PhonePe India Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0.
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
