package com.phonepe.growth.magazine;

import com.phonepe.growth.magazine.core.BaseMagazine;
import lombok.Builder;
import lombok.Data;

import java.util.Optional;

@Data
public class Magazine {
    private final String clientId;
    private final BaseMagazine baseMagazine;

    @Builder
    public Magazine(String clientId, BaseMagazine baseMagazine) {
        this.clientId = clientId;
        this.baseMagazine = baseMagazine;
    }

    public boolean prepare(String keyPrefix) {
        return baseMagazine.prepare(keyPrefix);
    }

    public boolean load(String keyPrefix, Object data) {
        return baseMagazine.load(keyPrefix, data);
    }

    public Optional<Object> fire(String keyPrefix) {
        return baseMagazine.fire(keyPrefix);
    }
}
