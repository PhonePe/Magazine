package com.phonepe.growth.magazine.commands;

import com.phonepe.growth.dlm.DistributedLockManager;
import com.phonepe.growth.dlm.exception.DLSException;
import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;

public class LockCommands {
    private final DistributedLockManager distributedLockManager;

    public LockCommands(final DistributedLockManager distributedLockManager) {
        this.distributedLockManager = distributedLockManager;
    }

    public boolean acquireLock(final String lockId) {
        try {
            return distributedLockManager.acquireLock(lockId);
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
    }

    public boolean releaseLock(final String lockId) {
        return distributedLockManager.releaseLock(lockId);
    }
}
