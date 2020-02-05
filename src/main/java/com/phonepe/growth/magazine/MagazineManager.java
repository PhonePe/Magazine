package com.phonepe.growth.magazine;

import com.phonepe.growth.dlm.DistributedLockManager;
import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
import com.phonepe.growth.magazine.util.LockUtils;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class MagazineManager {
    private final String clientId;
    private final Map<String, Magazine<?>> magazineMap = new HashMap<>();

    public MagazineManager(String clientId, DistributedLockManager distributedLockManager) {
        this.clientId = clientId;
        LockUtils.initialize(distributedLockManager);
    }

    public void refresh(List<Magazine<?>> magazines) {
        magazines.forEach(magazine -> magazineMap.put(magazine.getMagazineIdentifier(), magazine));
    }

    @SuppressWarnings("unchecked")
    public <T> Magazine<T> getMagazine(String magazineIdentifier, Class<T> klass) {
        try {
            return (Magazine<T>) (magazineMap.get(magazineIdentifier));
        } catch (ClassCastException e) {
            throw MagazineException.builder()
                    .message(String.format("Unable to provide magazine of required storage type for %s", magazineIdentifier))
                    .errorCode(ErrorCode.STORAGE_TYPE_MISMATCH)
                    .cause(e)
                    .build();
        } catch (Exception e) {
            throw MagazineException.builder()
                    .message(String.format("Magazine not found for identifier %s", magazineIdentifier))
                    .errorCode(ErrorCode.MAGAZINE_NOT_FOUND)
                    .cause(e)
                    .build();
        }
    }
}
