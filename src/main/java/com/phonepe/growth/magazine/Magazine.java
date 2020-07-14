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
    public Magazine(final String clientId, final BaseMagazineStorage<T> baseMagazineStorage,
                    final String magazineIdentifier) throws ExecutionException, RetryException {
        this.clientId = clientId;
        this.magazineIdentifier = magazineIdentifier;
        this.baseMagazineStorage = baseMagazineStorage;
        validateStorage(baseMagazineStorage);
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

    @SuppressWarnings("unchecked")
    private void validateStorage(final BaseMagazineStorage<T> baseMagazineStorage) throws ExecutionException, RetryException {
        baseMagazineStorage.getType().accept(new StorageTypeVisitor<Boolean>() {
            @Override
            public Boolean visitAerospike() throws ExecutionException, RetryException {
                final AerospikeStorage storage = (AerospikeStorage) baseMagazineStorage;

                final Record record = (Record) storage.getRetryerFactory().getRetryer().call(() ->
                        storage.getAerospikeClient().get(storage.getAerospikeClient().getReadPolicyDefault(),
                                new Key(storage.getNamespace(),
                                        storage.getMetaSetName(),
                                        String.join(Constants.KEY_DELIMITER, magazineIdentifier, Constants.SHARDS_BIN))));

                if (record == null) {
                    final WritePolicy writePolicy = storage.getAerospikeClient().getWritePolicyDefault();
                    writePolicy.expiration = Constants.SHARDS_DEFAULT_TTL;
                    storage.getRetryerFactory().getRetryer().call(() -> {
                        storage.getAerospikeClient().put(writePolicy,
                                new Key(storage.getNamespace(),
                                        storage.getMetaSetName(),
                                        String.join(Constants.KEY_DELIMITER, magazineIdentifier, Constants.SHARDS_BIN)),
                                new Bin(Constants.SHARDS_BIN, storage.getShards()));
                        return null;
                    });
                    return true;
                }

                final int storedShards = record.getInt(Constants.SHARDS_BIN);
                if (storedShards > storage.getShards()) {
                    throw MagazineException.builder()
                            .errorCode(ErrorCode.INVALID_SHARDS)
                            .message("Cannot decrease shards of a magazine.")
                            .build();
                }
                if (storedShards <= 1 && storage.getShards() > 1) {
                    throw MagazineException.builder()
                            .errorCode(ErrorCode.INVALID_SHARDS)
                            .message("Cannot convert unsharded to sharded magazine.")
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
}
