package com.phonepe.growth.magazine.common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author shantanu.tiwari
 * Created on 12/03/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FiredData<T> {
    private T data;
    private long firePointer;
    private Integer shard;
    private String magazineIdentifier;

    public String createAerospikeKey() {
        return shard != null ? String.join(Constants.KEY_DELIMITER,
                magazineIdentifier,
                Constants.SHARD_PREFIX,
                String.valueOf(shard),
                String.valueOf(firePointer))
                : String.join(Constants.KEY_DELIMITER, magazineIdentifier, String.valueOf(firePointer));
    }
}
