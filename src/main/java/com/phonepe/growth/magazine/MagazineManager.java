package com.phonepe.growth.magazine;

import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Data
public class MagazineManager {
    private final String clientId;
    private final Map<String, Magazine<?>> magazineMap = new HashMap<>();

    public MagazineManager(final String clientId) {
        this.clientId = clientId;
    }

    public void refresh(final List<Magazine<?>> magazines) {
        magazines.forEach(magazine -> magazineMap.put(magazine.getMagazineIdentifier(), magazine));
    }

    @SuppressWarnings("unchecked")
    public <T> Magazine<T> getMagazine(final String magazineIdentifier) {
        Magazine<T> magazine =  (Magazine<T>) (magazineMap.get(magazineIdentifier));
        if (Objects.isNull(magazine)) {
            throw MagazineException.builder()
                    .message(String.format("Magazine not found for identifier %s", magazineIdentifier))
                    .errorCode(ErrorCode.MAGAZINE_NOT_FOUND)
                    .build();
        }
        return magazine;
    }
}
