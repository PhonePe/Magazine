package com.phonepe.growth.magazine;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.github.rholder.retry.RetryException;
import com.phonepe.growth.magazine.common.Constants;
import com.phonepe.growth.magazine.common.MetaData;
import com.phonepe.growth.magazine.core.BaseMagazineStorage;
import com.phonepe.growth.magazine.core.StorageTypeVisitor;
import com.phonepe.growth.magazine.exception.ErrorCode;
import com.phonepe.growth.magazine.exception.MagazineException;
import com.phonepe.growth.magazine.impl.aerospike.AerospikeStorage;
import lombok.Builder;
import lombok.Data;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Data
public class Magazine<T> {
    private final String clientId;
    private final BaseMagazineStorage<T> baseMagazineStorage;
    private final String magazineIdentifier;

    @Builder
    public Magazine(final String clientId, final BaseMagazineStorage<T> baseMagazineStorage, final String magazineIdentifier) throws Exception {
        this.clientId = clientId;
        this.magazineIdentifier = magazineIdentifier;
        this.baseMagazineStorage = baseMagazineStorage;
        validateStorage(baseMagazineStorage);
    }

    @SuppressWarnings("unchecked")
    private void validateStorage(final BaseMagazineStorage<T> baseMagazineStorage) throws Exception {
        baseMagazineStorage.getType().accept(new StorageTypeVisitor<Boolean>() {
            @Override
            public Boolean visitAerospike() throws ExecutionException, RetryException {
                final AerospikeStorage storage = (AerospikeStorage) baseMagazineStorage;
                final Record record = (Record) storage.getRetryerFactory().getRetryer().call(() ->
                        storage.getAerospikeClient().get(storage.getAerospikeClient().getReadPolicyDefault(),
                                new Key(storage.getNamespace(), storage.getMetaSetName(),
                                        String.join(Constants.KEY_DELIMITER, magazineIdentifier, Constants.IS_SHARDED))));
                if (record == null) {
                    final WritePolicy writePolicy = storage.getAerospikeClient().getWritePolicyDefault();
                    writePolicy.expiration = Constants.IS_SHARDED_DEFAULT_TTL;
                    storage.getRetryerFactory().getRetryer().call(() -> {
                        storage.getAerospikeClient().put(writePolicy, new Key(storage.getNamespace(), storage.getMetaSetName(),
                                        String.join(Constants.KEY_DELIMITER, magazineIdentifier, Constants.IS_SHARDED)),
                                new Bin(Constants.IS_SHARDED, storage.getShards() > 1));
                        return null;
                    });
                    return true;
                }

                final boolean isSharded = record.getBoolean(Constants.IS_SHARDED);
                if ((isSharded && storage.getShards() <= 1) || (!isSharded && storage.getShards() > 1)) {
                    throw MagazineException.builder()
                            .errorCode(ErrorCode.INVALID_SHARDS)
                            .message("Cannot convert unsharded to sharded magazine or vice-versa")
                            .build();
                }

                return true;
            }

            @Override
            public Boolean visitHBase() {
                throw new UnsupportedOperationException();
            }
        });
    }

    public boolean load(final T data) {
        return baseMagazineStorage.load(magazineIdentifier, data);
    }

    public boolean reload(final T data) {
        return baseMagazineStorage.reload(magazineIdentifier, data);
    }

    public Optional<T> fire() {
        return baseMagazineStorage.fire(magazineIdentifier);
    }

    public Map<String, MetaData> getMetaData() {
        return baseMagazineStorage.getMetaData(magazineIdentifier);
    }
}
