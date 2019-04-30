package com.phonepe.growth.magazine;

import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class MagazineManager {
    private final String clientId;
    private final Map<String, Magazine<?>> magazineMap = new HashMap<>();

    public MagazineManager(String clientId) {
        this.clientId = clientId;
    }

    public void refresh(List<Magazine<?>> magazines) {
        magazines.forEach(magazine -> magazineMap.put(magazine.getMagazineIdentifier(), magazine));
    }

    public void ensureMagazine(Magazine<?> magazine) {
        if(!magazineMap.containsKey(magazine.getMagazineIdentifier())) {
            magazineMap.put(magazine.getMagazineIdentifier(), magazine);
        }
    }

    public Magazine<?> getMagazine(String magazineIdentifier) {
        try {
            return magazineMap.get(magazineIdentifier);
        } catch (Exception e) {
            throw MagazineException.builder()
                    .message(String.format("Magazine not found for identifier %s", magazineIdentifier))
                    .errorCode(ErrorCode.MAGAZINE_NOT_FOUND)
                    .cause(e)
                    .build();
        }
    }
}
