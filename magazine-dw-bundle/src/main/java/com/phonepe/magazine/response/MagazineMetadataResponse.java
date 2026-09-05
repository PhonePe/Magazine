package com.phonepe.magazine.response;

import java.util.Map;

public record MagazineMetadataResponse(String identifier, Map<String, ShardMetadata> shards, ShardMetadata totals) {
}
