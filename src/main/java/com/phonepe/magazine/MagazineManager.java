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

import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
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

    /**
     * rebuilds magazineMap wrt the list of magazines provided
     *
     * @param magazines
     */
    public void refresh(final List<Magazine<?>> magazines) {
        magazines.forEach(magazine -> magazineMap.put(magazine.getMagazineIdentifier(), magazine));
    }

    /**
     *  function provides magazine corresponding to magazine identifier
     *
     * @param magazineIdentifier
     * @return magazine corresponding to provided magazineIdentifier
     * @param <T>
     */
    @SuppressWarnings("unchecked")
    public <T> Magazine<T> getMagazine(final String magazineIdentifier) {
        Magazine<T> magazine = (Magazine<T>) (magazineMap.get(magazineIdentifier));
        if (Objects.isNull(magazine)) {
            throw MagazineException.builder()
                    .message(String.format("Magazine not found for identifier %s", magazineIdentifier))
                    .errorCode(ErrorCode.MAGAZINE_NOT_FOUND)
                    .build();
        }
        return magazine;
    }
}
