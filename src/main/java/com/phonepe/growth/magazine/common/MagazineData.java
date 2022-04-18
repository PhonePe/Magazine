package com.phonepe.growth.magazine.common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * @author shantanu.tiwari
 * Created on 12/03/22
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MagazineData<T> {
    private T data;
    private long firePointer;
    private Integer shard;
    private String magazineIdentifier;

    public String createAerospikeKey() {
        return Objects.nonNull(shard)
                ? String.format("%s_%s_%d_%d", magazineIdentifier, Constants.SHARD_PREFIX, shard, firePointer)
                : String.format("%s_%d", magazineIdentifier, firePointer);
    }
}