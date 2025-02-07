package com.phonepe.magazine.util;

import com.phonepe.dlm.lock.level.LockLevel;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.scope.MagazineScope;
import com.phonepe.magazine.scope.MagazineScope.Visitor;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CommonUtils {

    private static final String LOCAL_SCOPE_SET_FORMAT = "%s_%s";

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

    public static void validateMagazineScope(final MagazineScope scope) {
        if (scope == MagazineScope.GLOBAL) {
            throw MagazineException.builder()
                    .errorCode(ErrorCode.NOT_IMPLEMENTED)
                    .message("Global scope is not implemented.")
                    .build();
        }
    }

    public static String resolveSetName(
            final String setName,
            final String farmId,
            final MagazineScope scope) {
        return scope.accept(new Visitor<>() {
            @Override
            public String visitLocal() {
                return String.format(LOCAL_SCOPE_SET_FORMAT, farmId, setName);
            }

            // Default scope for backward compatibility will be global.
            // Will support local scope backward compatibility according to client's use case if any
            @Override
            public String visitGlobal() {
                return setName;
            }
        });
    }

}
