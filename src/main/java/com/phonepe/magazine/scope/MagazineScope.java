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
