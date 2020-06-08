package com.phonepe.growth.magazine;

import com.phonepe.growth.dlm.DistributedLockManager;
import com.phonepe.growth.magazine.util.LockUtils;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class MagazineManager {
    private final String clientId;
    private final Map<String, Magazine<?>> magazineMap = new HashMap<>();

    public MagazineManager(final String clientId, final DistributedLockManager distributedLockManager) {
        this.clientId = clientId;
        LockUtils.initialize(distributedLockManager);
    }

    public void refresh(final List<Magazine<?>> magazines) {
        magazines.forEach(magazine -> magazineMap.put(magazine.getMagazineIdentifier(), magazine));
    }

    @SuppressWarnings("unchecked")
    public <T> Magazine<T> getMagazine(final String magazineIdentifier) {
        return (Magazine<T>) (magazineMap.get(magazineIdentifier));
    }
}
