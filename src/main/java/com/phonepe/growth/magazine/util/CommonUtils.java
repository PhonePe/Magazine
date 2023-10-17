package com.phonepe.growth.magazine.util;

import com.phonepe.growth.dlm.lock.level.LockLevel;
import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
import com.phonepe.growth.magazine.scope.MagazineScope;
import com.phonepe.growth.magazine.scope.MagazineScope.Visitor;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CommonUtils {

    public static LockLevel resolveLockLevel(final MagazineScope scope) {
        return scope.accept(new Visitor<>() {
            @Override
            public LockLevel visitLocal() {
                return LockLevel.DC;
            }

            @Override
            public LockLevel visitGlobal() {
                return LockLevel.XDC;
            }
        });
    }

    public static <T> void validateMagazineScope(final MagazineScope scope) {
        if (scope == MagazineScope.GLOBAL) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.NOT_IMPLEMENTED)
                    .message("Global scope is not implemented.")
                    .build();
        }
    }

}
