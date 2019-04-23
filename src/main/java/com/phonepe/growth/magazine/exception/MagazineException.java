package com.phonepe.growth.magazine.exception;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class MagazineException extends RuntimeException {
    private static final long serialVersionUID = 3941797721266293207L;
    private final ErrorCode errorCode;

    @Builder
    public MagazineException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public static MagazineException propagate(final Throwable throwable) {
        return propagate("Error occurred", throwable);
    }

    public static MagazineException propagate(final String message, final Throwable throwable) {
        if (throwable instanceof MagazineException) {
            return (MagazineException) throwable;
        } else if (throwable.getCause() != null && throwable.getCause() instanceof MagazineException) {
            return (MagazineException) throwable.getCause();
        }
        return new MagazineException(ErrorCode.INTERNAL_ERROR, message, throwable);
    }
}
