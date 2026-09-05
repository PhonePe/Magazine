package com.phonepe.magazine.demo;

import com.phonepe.magazine.config.MagazineBundleConfiguration;
import io.dropwizard.core.Configuration;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public final class MagazineDashboardDemoConfiguration extends Configuration {

    @Valid
    @NotNull
    private MagazineBundleConfiguration magazineBundle = new MagazineBundleConfiguration();
}
