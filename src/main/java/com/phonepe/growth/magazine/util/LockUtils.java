package com.phonepe.growth.magazine.util;

import com.phonepe.growth.dlm.DistributedLockManager;
import com.phonepe.growth.dlm.exception.DLSException;
import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class LockUtils {
    private static DistributedLockManager distributedLockManager;

    public static void initialize(DistributedLockManager dlm) {
        distributedLockManager = dlm;
    }

    public static boolean acquireLock(String lockId) {
        boolean result;
        try {
            result = distributedLockManager.acquireLock(lockId);
        } catch (DLSException e) {
            if (com.phonepe.growth.dlm.exception.ErrorCode.LOCK_UNAVAILABLE.equals(e.getErrorCode())) {
                throw MagazineException.builder()
                        .errorCode(ErrorCode.ACTION_DENIED_PARALLEL_ATTEMPT)
                        .message(String.format("Error acquiring lock - %s", lockId))
                        .cause(e)
                        .build();

            }
            throw MagazineException.propagate(e); // Generic exception propagation.
        }
        return result;
    }

    public static boolean releaseLock(String lockId) {
        return distributedLockManager.releaseLock(lockId);
    }
}
