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

package com.phonepe.magazine.scope;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum MagazineScope {
    LOCAL(MagazineScope.LOCAL_TEXT) {
        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visitLocal();
        }
    },
    GLOBAL(MagazineScope.GLOBAL_TEXT) {
        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.visitGlobal();
        }
    };

    public static final String LOCAL_TEXT = "LOCAL";
    public static final String GLOBAL_TEXT = "GLOBAL";

    @Getter
    private final String value;

    public abstract <T> T accept(Visitor<T> visitor);

    public interface Visitor<T> {

        T visitLocal();

        T visitGlobal();
    }

}
