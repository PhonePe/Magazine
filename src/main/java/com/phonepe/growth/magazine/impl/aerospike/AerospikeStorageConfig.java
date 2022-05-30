package com.phonepe.growth.magazine.impl.aerospike;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

/**
 * @author shantanu.tiwari
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AerospikeStorageConfig {
    @NotBlank
    private String namespace;
    @NotBlank
    private String metaSetName;
    @NotBlank
    private String dataSetName;
    @Min(-2)
    @Builder.Default
    private int recordTtl = 30 * 24 * 60 * 60;  // 30 days default ttl
    @Min(1)
    @Builder.Default
    private int shards = 64; //Default 64 shards in a magazine
    @Min(-2)
    @Builder.Default
    private int metaDataTtl = 2 * 30 *24 * 60 * 60; // 2 months default TTL
}
