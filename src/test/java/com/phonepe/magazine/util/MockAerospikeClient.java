/**
 * Copyright (c) 2025 Original Author(s), PhonePe India Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.phonepe.magazine.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.IndexListener;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchUDFPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.QueryListener;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.google.common.collect.Lists;
import com.phonepe.magazine.exception.MagazineException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MockAerospikeClient implements IAerospikeClient {

    private final Map<Key, Record> data = new ConcurrentHashMap<>();

    /**
     * Close all client connections to database server nodes.
     */
    public void close() {
        data.clear();
    }

    /**
     * Determine if we are ready to talk to the database server cluster. <br>
     * Note: Mock always returns true.
     *
     * @return <code>true</code> if cluster is ready, <code>false</code> if
     * cluster is not ready
     */
    public boolean isConnected() {
        return true;
    }

    /**
     * Return array of active server nodes in the cluster. <br>
     * Always returns null - not implemented.
     *
     * @return array of active nodes
     */
    public Node[] getNodes() {
        return new Node[]{};
    }

    /**
     * Return list of active server node names in the cluster.
     *
     * @return list of active node names
     */
    public List<String> getNodeNames() {
        return Lists.newLinkedList();
    }

    /**
     * Return node given its name.
     *
     * @throws AerospikeException.InvalidNode if node does not exist.
     */
    public Node getNode(String nodeName) throws AerospikeException.InvalidNode {
        throw new AerospikeException.InvalidNode("Invalid node");
    }

    @Override
    public ClusterStats getClusterStats() {
        return null;
    }

    @Override
    public Cluster getCluster() {
        return null;
    }

    /**
     * Write record bin(s). The policy specifies the transaction timeout, record
     * expiration and how the transaction is handled when the record already
     * exists.
     *
     * @param policy write configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @param bins   array of bin name/value pairs
     * @throws AerospikeException if write fails
     */
    public void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        if (GenerationPolicy.EXPECT_GEN_EQUAL.equals(policy.generationPolicy) && exists(policy, key)) {
            throw new AerospikeException(ResultCode.GENERATION_ERROR);
        }
        data.put(key, new Record(convertToMap(bins), 0, 0));
    }

    @Override
    public void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Append bin string values to existing record bin values. The policy
     * specifies the transaction timeout, record expiration and how the
     * transaction is handled when the record already exists. This call only
     * works for string values.
     *
     * @param policy write configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @param bins   array of bin name/value pairs
     * @throws AerospikeException if append fails
     */
    public void append(WritePolicy policy, Key key, Bin... bins)
            throws AerospikeException {
        // If Key is not present, create a new record.
        // If Bin is present but not a string, throw
        // com.aerospike.client.AerospikeException: Error Code 12: Bin type
        // error
        // else, append the string.

        throw new UnsupportedOperationException(
                "append is not supported in MockAerospike");

    }

    @Override
    public void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Prepend bin string values to existing record bin values. The policy
     * specifies the transaction timeout, record expiration and how the
     * transaction is handled when the record already exists. This call works
     * only for string values.
     *
     * @param policy write configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @param bins   array of bin name/value pairs
     * @throws AerospikeException if prepend fails
     */
    public void prepend(WritePolicy policy, Key key, Bin... bins)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "prepend is not supported in MockAerospike");

    }

    @Override
    public void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Add integer bin values to existing record bin values. The policy
     * specifies the transaction timeout, record expiration and how the
     * transaction is handled when the record already exists. This call only
     * works for integer values.
     *
     * @param policy write configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @param bins   array of bin name/value pairs
     * @throws AerospikeException if add fails
     */
    public void add(WritePolicy policy, Key key, Bin... bins)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "add is not supported in MockAerospike");

    }

    @Override
    public void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Delete record for specified key. The policy specifies the transaction
     * timeout.
     *
     * @param policy delete configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @return whether record existed on server before deletion
     * @throws AerospikeException if delete fails
     */
    public boolean delete(WritePolicy policy, Key key)
            throws AerospikeException {
        if (data.containsKey(key)) {
            data.remove(key);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public BatchResults delete(BatchPolicy batchPolicy,
            BatchDeletePolicy batchDeletePolicy,
            Key[] keys) throws AerospikeException {
        return null;
    }

    @Override
    public void delete(EventLoop eventLoop,
            BatchRecordArrayListener batchRecordArrayListener,
            BatchPolicy batchPolicy,
            BatchDeletePolicy batchDeletePolicy,
            Key[] keys) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void delete(EventLoop eventLoop,
            BatchRecordSequenceListener batchRecordSequenceListener,
            BatchPolicy batchPolicy,
            BatchDeletePolicy batchDeletePolicy,
            Key[] keys) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Reset record's time to expiration using the policy's expiration. Fail if
     * the record does not exist.
     *
     * @param policy write configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @throws AerospikeException if touch fails
     */
    public void touch(WritePolicy policy, Key key) throws AerospikeException {
        throw new UnsupportedOperationException(
                "touch is not supported in MockAerospike");

    }

    @Override
    public void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Determine if a record key exists. The policy can be used to specify
     * timeouts.
     *
     * @param policy generic configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @return whether record exists or not
     * @throws AerospikeException if command fails
     */
    public boolean exists(Policy policy, Key key) throws AerospikeException {
        return data.containsKey(key);
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Check if multiple record keys exist in one batch call. The returned
     * boolean array is in positional order with the original key array order.
     * The policy can be used to specify timeouts and maximum concurrent
     * threads.
     *
     * @param policy batch configuration parameters, pass in null for defaults
     * @param keys   array of unique record identifiers
     * @return array key/existence status pairs
     * @throws AerospikeException if command fails
     */
    public boolean[] exists(BatchPolicy policy, Key[] keys)
            throws AerospikeException {
        boolean[] result = new boolean[keys.length];
        for (int idx = 0; idx < keys.length; idx++) {
            result[idx] = data.containsKey(keys[idx]);
        }

        return result;
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Read entire record for specified key. The policy can be used to specify
     * timeouts.
     *
     * @param policy generic configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @return if found, return record instance. If not found, return null.
     * @throws AerospikeException if read fails
     */
    public Record get(Policy policy, Key key) throws AerospikeException {
        return data.get(key);
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Read record header and bins for specified key. The policy can be used to
     * specify timeouts.
     *
     * @param policy   generic configuration parameters, pass in null for defaults
     * @param key      unique record identifier
     * @param binNames bins to retrieve
     * @return if found, return record instance. If not found, return null.
     * @throws AerospikeException if read fails
     */
    public Record get(Policy policy, Key key, String... binNames)
            throws AerospikeException {
        final Record magazineRecord = data.get(key);
        if (magazineRecord == null) {
            return null;
        } else {
            // filter bins.
            Map<String, Object> filteredBins = new HashMap<>();
            for (String bin : binNames) {
                filteredBins.put(bin, magazineRecord.bins.get(bin));
            }
            return new Record(filteredBins, magazineRecord.generation,
                    magazineRecord.expiration);
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Read record generation and expiration only for specified key. Bins are
     * not read. The policy can be used to specify timeouts.
     *
     * @param policy generic configuration parameters, pass in null for defaults
     * @param key    unique record identifier
     * @return if found, return record instance. If not found, return null.
     * @throws AerospikeException if read fails
     */
    public Record getHeader(Policy policy, Key key) throws AerospikeException {
        Record magazineRecord = data.get(key);

        if (magazineRecord == null) {
            return null;
        } else {
            return new Record(null, magazineRecord.generation, magazineRecord.expiration);
        }

    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Read multiple records for specified keys in one batch call. The returned
     * records are in positional order with the original key array order. If a
     * key is not found, the positional record will be null. The policy can be
     * used to specify timeouts.
     *
     * @param policy generic configuration parameters, pass in null for defaults
     * @param keys   array of unique record identifiers
     * @return array of records
     * @throws AerospikeException if read fails
     * Use {@link #get(BatchPolicy, Key[])} instead.
     */
    private Record[] get(Policy policy, Key[] keys) throws AerospikeException {
        Record[] records = new Record[keys.length];
        for (int idx = 0; idx < records.length; idx++) {
            records[idx] = get(policy, keys[idx]);
        }
        return records;
    }

    /**
     * Read multiple records for specified keys in one batch call. The returned
     * records are in positional order with the original key array order. If a
     * key is not found, the positional record will be null. The policy can be
     * used to specify timeouts and maximum concurrent threads.
     *
     * @param policy batch configuration parameters, pass in null for defaults
     * @param keys   array of unique record identifiers
     * @return array of records
     * @throws AerospikeException if read fails
     */
    public Record[] get(BatchPolicy policy, Key[] keys)
            throws AerospikeException {
        return get((Policy) policy, keys);
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Read multiple record headers and bins for specified keys in one batch
     * call. The returned records are in positional order with the original key
     * array order. If a key is not found, the positional record will be null.
     * The policy can be used to specify timeouts.
     *
     * @param policy   generic configuration parameters, pass in null for defaults
     * @param keys     array of unique record identifiers
     * @param binNames array of bins to retrieve
     * @return array of records
     * @throws AerospikeException if read fails
     * Use {@link #get(BatchPolicy, Key[], String...)} instead.
     */
    private Record[] get(Policy policy, Key[] keys, String... binNames)
            throws AerospikeException {
        Record[] records = new Record[keys.length];
        for (int idx = 0; idx < records.length; idx++) {
            records[idx] = get(policy, keys[idx], binNames);
        }
        return records;
    }

    /**
     * Read multiple record headers and bins for specified keys in one batch
     * call. The returned records are in positional order with the original key
     * array order. If a key is not found, the positional record will be null.
     * The policy can be used to specify timeouts and maximum concurrent
     * threads.
     *
     * @param policy   batch configuration parameters, pass in null for defaults
     * @param keys     array of unique record identifiers
     * @param binNames array of bins to retrieve
     * @return array of records
     * @throws AerospikeException if read fails
     */
    public Record[] get(BatchPolicy policy, Key[] keys, String... binNames)
            throws AerospikeException {
        return get((Policy) policy, keys, binNames);
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public Record[] get(BatchPolicy batchPolicy,
            Key[] keys,
            Operation... operations) throws AerospikeException {
        return new Record[0];
    }

    @Override
    public void get(EventLoop eventLoop,
            RecordArrayListener recordArrayListener,
            BatchPolicy batchPolicy,
            Key[] keys,
            Operation... operations) throws AerospikeException {

    }

    @Override
    public void get(EventLoop eventLoop,
            RecordSequenceListener recordSequenceListener,
            BatchPolicy batchPolicy,
            Key[] keys,
            Operation... operations) throws AerospikeException {

    }

    /**
     * Read multiple record header data for specified keys in one batch call.
     * The returned records are in positional order with the original key array
     * order. If a key is not found, the positional record will be null. The
     * policy can be used to specify timeouts.
     *
     * @param policy generic configuration parameters, pass in null for defaults
     * @param keys   array of unique record identifiers
     * @return array of records
     * @throws AerospikeException if read fails
     */
    private Record[] getHeader(Policy policy, Key[] keys)
            throws AerospikeException {
        Record[] records = new Record[keys.length];
        for (int idx = 0; idx < records.length; idx++) {
            records[idx] = getHeader(policy, keys[idx]);
        }
        return records;
    }

    /**
     * Read multiple record header data for specified keys in one batch call.
     * The returned records are in positional order with the original key array
     * order. If a key is not found, the positional record will be null. The
     * policy can be used to specify timeouts and maximum concurrent threads.
     *
     * @param policy batch configuration parameters, pass in null for defaults
     * @param keys   array of unique record identifiers
     * @return array of records
     * @throws AerospikeException if read fails
     */
    public Record[] getHeader(BatchPolicy policy, Key[] keys)
            throws AerospikeException {
        return getHeader((Policy) policy, keys);
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Perform multiple read/write operations on a single key in one batch call.
     * An example would be to add an integer value to an existing record and
     * then read the result, all in one database call.
     * <p>
     * Write operations are always performed first, regardless of operation
     * order relative to read operations.
     *
     * @param policy     write configuration parameters, pass in null for defaults
     * @param key        unique record identifier
     * @param operations database operations to perform
     * @return record if there is a read in the operations list
     * @throws AerospikeException if command fails
     */
    public Record operate(WritePolicy policy, Key key, Operation... operations)
            throws AerospikeException {
        return Arrays.stream(operations).map(operation -> applyOperation(operation, key))
                .findFirst().orElseThrow(() -> MagazineException.builder().build());
    }

    @Override
    public void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public boolean operate(BatchPolicy batchPolicy,
            List<BatchRecord> list) throws AerospikeException {
        return false;
    }

    @Override
    public void operate(EventLoop eventLoop,
            BatchOperateListListener batchOperateListListener,
            BatchPolicy batchPolicy,
            List<BatchRecord> list) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void operate(EventLoop eventLoop,
            BatchRecordSequenceListener batchRecordSequenceListener,
            BatchPolicy batchPolicy,
            List<BatchRecord> list) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public BatchResults operate(BatchPolicy batchPolicy,
            BatchWritePolicy batchWritePolicy,
            Key[] keys,
            Operation... operations) throws AerospikeException {
        return null;
    }

    @Override
    public void operate(EventLoop eventLoop,
            BatchRecordArrayListener batchRecordArrayListener,
            BatchPolicy batchPolicy,
            BatchWritePolicy batchWritePolicy,
            Key[] keys,
            Operation... operations) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void operate(EventLoop eventLoop,
            BatchRecordSequenceListener batchRecordSequenceListener,
            BatchPolicy batchPolicy,
            BatchWritePolicy batchWritePolicy,
            Key[] keys,
            Operation... operations) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Read all records in specified namespace and set. If the policy's
     * <code>concurrentNodes</code> is specified, each server node will be read
     * in parallel. Otherwise, server nodes are read in series.
     * <p>
     * This call will block until the scan is complete - callbacks are made
     * within the scope of this call.
     *
     * @param policy    scan configuration parameters, pass in null for defaults
     * @param namespace namespace - equivalent to database name
     * @param setName   optional set name - equivalent to database table
     * @param callback  read callback method - called with record data
     * @param binNames  optional bin to retrieve. All bins will be returned if not
     *                  specified. Aerospike 2 servers ignore this parameter.
     * @throws AerospikeException if scan fails
     */
    public void scanAll(ScanPolicy policy, String namespace, String setName,
                        ScanCallback callback, String... binNames)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "scanAll is not supported in MockAerospike");

    }

    @Override
    public void scanAll(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, String namespace, String setName, String... binNames) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Read all records in specified namespace and set for one node only. The
     * node is specified by name.
     * <p>
     * This call will block until the scan is complete - callbacks are made
     * within the scope of this call.
     *
     * @param policy    scan configuration parameters, pass in null for defaults
     * @param nodeName  server node name
     * @param namespace namespace - equivalent to database name
     * @param setName   optional set name - equivalent to database table
     * @param callback  read callback method - called with record data
     * @param binNames  optional bin to retrieve. All bins will be returned if not
     *                  specified. Aerospike 2 servers ignore this parameter.
     * @throws AerospikeException if scan fails
     */
    public void scanNode(ScanPolicy policy, String nodeName, String namespace,
                         String setName, ScanCallback callback, String... binNames)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "scanNode is not supported in MockAerospike");

    }

    /**
     * Read all records in specified namespace and set for one node only.
     * <p>
     * This call will block until the scan is complete - callbacks are made
     * within the scope of this call.
     *
     * @param policy    scan configuration parameters, pass in null for defaults
     * @param node      server node
     * @param namespace namespace - equivalent to database name
     * @param setName   optional set name - equivalent to database table
     * @param callback  read callback method - called with record data
     * @param binNames  optional bin to retrieve. All bins will be returned if not
     *                  specified. Aerospike 2 servers ignore this parameter.
     * @throws AerospikeException if transaction fails
     */
    public void scanNode(ScanPolicy policy, Node node, String namespace,
                         String setName, ScanCallback callback, String... binNames)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "scanNode is not supported in MockAerospike");

    }

    @Override
    public void scanPartitions(ScanPolicy scanPolicy,
            PartitionFilter partitionFilter,
            String s,
            String s1,
            ScanCallback scanCallback,
            String... strings) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void scanPartitions(EventLoop eventLoop,
            RecordSequenceListener recordSequenceListener,
            ScanPolicy scanPolicy,
            PartitionFilter partitionFilter,
            String s,
            String s1,
            String... strings) throws AerospikeException {
        // Do Nothing
    }


    /**
     * Register package containing user defined functions with server. This
     * asynchronous server call will return before command is complete. The user
     * can optionally wait for command completion by using the returned
     * RegisterTask instance.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy     generic configuration parameters, pass in null for defaults
     * @param clientPath path of client file containing user defined functions,
     *                   relative to current directory
     * @param serverPath path to store user defined functions on the server, relative
     *                   to configured script directory.
     * @param language   language of user defined functions
     * @throws AerospikeException if register fails
     */
    public RegisterTask register(Policy policy, String clientPath,
                                 String serverPath, Language language) throws AerospikeException {
        throw new UnsupportedOperationException(
                "register is not supported in MockAerospike");
    }

    /**
     * Execute user defined function on server and return results. The function
     * operates on a single record. The package name is used to locate the udf
     * file location:
     * <p>
     * udf file = {server udf dir}/${package name}.lua
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy       generic configuration parameters, pass in null for defaults
     * @param key          unique record identifier
     * @param packageName  server package name where user defined function resides
     * @param functionName user defined function
     * @param args         arguments passed in to user defined function
     * @return return value of user defined function
     * @throws AerospikeException if transaction fails
     * @deprecated Use
     * {@link #execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args)}
     * instead.
     */
    public Object execute(Policy policy, Key key, String packageName,
                          String functionName, Value... args) throws AerospikeException {
        throw new UnsupportedOperationException(
                "execute is not supported in MockAerospike");
    }

    /**
     * Execute user defined function on server and return results. The function
     * operates on a single record. The package name is used to locate the udf
     * file location:
     * <p>
     * udf file = ${server udf dir}/${package name}.lua
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy       write configuration parameters, pass in null for defaults
     * @param key          unique record identifier
     * @param packageName  server package name where user defined function resides
     * @param functionName user defined function
     * @param args         arguments passed in to user defined function
     * @return return value of user defined function
     * @throws AerospikeException if transaction fails
     */
    public Object execute(WritePolicy policy, Key key, String packageName,
                          String functionName, Value... args) throws AerospikeException {
        throw new UnsupportedOperationException(
                "execute is not supported in MockAerospike");
    }

    @Override
    public void execute(EventLoop eventLoop, ExecuteListener listener, WritePolicy policy, Key key, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public BatchResults execute(BatchPolicy batchPolicy,
            BatchUDFPolicy batchUDFPolicy,
            Key[] keys,
            String s,
            String s1,
            Value... values) throws AerospikeException {
        return null;
    }

    @Override
    public void execute(EventLoop eventLoop,
            BatchRecordArrayListener batchRecordArrayListener,
            BatchPolicy batchPolicy,
            BatchUDFPolicy batchUDFPolicy,
            Key[] keys,
            String s,
            String s1,
            Value... values) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void execute(EventLoop eventLoop,
            BatchRecordSequenceListener batchRecordSequenceListener,
            BatchPolicy batchPolicy,
            BatchUDFPolicy batchUDFPolicy,
            Key[] keys,
            String s,
            String s1,
            Value... values) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Apply user defined function on records that match the statement filter.
     * Records are not returned to the client. This asynchronous server call
     * will return before command is complete. The user can optionally wait for
     * command completion by using the returned ExecuteTask instance.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy       scan configuration parameters, pass in null for defaults
     * @param statement    record filter
     * @param packageName  server package where user defined function resides
     * @param functionName function name
     * @param functionArgs to pass to function name, if any
     * @throws AerospikeException if command fails
     * @deprecated Use
     * {@link #execute(WritePolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs)}
     * instead.
     */
    public ExecuteTask execute(Policy policy, Statement statement,
                               String packageName, String functionName, Value... functionArgs)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "execute is not supported in MockAerospike");
    }

    /**
     * Apply user defined function on records that match the statement filter.
     * Records are not returned to the client. This asynchronous server call
     * will return before command is complete. The user can optionally wait for
     * command completion by using the returned ExecuteTask instance.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy       write configuration parameters, pass in null for defaults
     * @param statement    record filter
     * @param packageName  server package where user defined function resides
     * @param functionName function name
     * @param functionArgs to pass to function name, if any
     * @throws AerospikeException if command fails
     */
    public ExecuteTask execute(WritePolicy policy, Statement statement,
                               String packageName, String functionName, Value... functionArgs)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "execute is not supported in MockAerospike");
    }

    @Override
    public ExecuteTask execute(WritePolicy writePolicy,
            Statement statement,
            Operation... operations) throws AerospikeException {
        return null;
    }

    /**
     * Execute query on all server nodes and return record iterator. The query
     * executor puts records on a queue in separate threads. The calling thread
     * concurrently pops records off the queue through the record iterator.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy    generic configuration parameters, pass in null for defaults
     * @param statement database query command
     * @return record iterator
     * @throws AerospikeException if query fails
     */
    public RecordSet query(QueryPolicy policy, Statement statement)
            throws AerospikeException {
        return null;
    }

    @Override
    public void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void query(QueryPolicy queryPolicy,
            Statement statement,
            QueryListener queryListener) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void query(QueryPolicy queryPolicy,
            Statement statement,
            PartitionFilter partitionFilter,
            QueryListener queryListener) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Execute query on a single server node and return record iterator. The
     * query executor puts records on a queue in a separate thread. The calling
     * thread concurrently pops records off the queue through the record
     * iterator.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy    generic configuration parameters, pass in null for defaults
     * @param statement database query command
     * @return record iterator
     * @throws AerospikeException if query fails
     */
    public RecordSet queryNode(QueryPolicy policy, Statement statement,
                               Node node) throws AerospikeException {
        throw new UnsupportedOperationException(
                "queryNode is not supported in MockAerospike");
    }

    @Override
    public RecordSet queryPartitions(QueryPolicy queryPolicy,
            Statement statement,
            PartitionFilter partitionFilter) throws AerospikeException {
        return null;
    }

    @Override
    public void queryPartitions(EventLoop eventLoop,
            RecordSequenceListener recordSequenceListener,
            QueryPolicy queryPolicy,
            Statement statement,
            PartitionFilter partitionFilter) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Execute query, apply statement's aggregation function, and return result
     * iterator. The query executor puts results on a queue in separate threads.
     * The calling thread concurrently pops results off the queue through the
     * result iterator.
     * <p>
     * The aggregation function is called on both server and client (final
     * reduce). Therefore, the Lua script files must also reside on both server
     * and client. The package name is used to locate the udf file location:
     * </p>
     * udf file = ${udf dir}/${package name}.lua
     * <p>
     * This method is only supported by Aerospike 3 servers.
     * </p>
     *
     * @param policy       generic configuration parameters, pass in null for defaults
     * @param statement    database query command
     * @param packageName  server package where user defined function resides
     * @param functionName aggregation function name
     * @param functionArgs arguments to pass to function name, if any
     * @return result iterator
     * @throws AerospikeException if query fails
     */
    public ResultSet queryAggregate(QueryPolicy policy, Statement statement,
                                    String packageName, String functionName, Value... functionArgs)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "queryAggregate is not supported in MockAerospike");
    }

    /**
     * Create scalar secondary index. This asynchronous server call will return
     * before command is complete. The user can optionally wait for command
     * completion by using the returned IndexTask instance.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy    generic configuration parameters, pass in null for defaults
     * @param namespace namespace - equivalent to database name
     * @param setName   optional set name - equivalent to database table
     * @param indexName name of secondary index
     * @param binName   bin name that data is indexed on
     * @param indexType underlying data type of secondary index
     * @throws AerospikeException if index create fails
     */
    public IndexTask createIndex(Policy policy, String namespace,
                                 String setName, String indexName, String binName,
                                 IndexType indexType) throws AerospikeException {
        throw new AerospikeException(200);
    }

    @Override
    public IndexTask createIndex(Policy policy,
            String s,
            String s1,
            String s2,
            String s3,
            IndexType indexType,
            IndexCollectionType indexCollectionType,
            CTX... ctxes) throws AerospikeException {
        return null;
    }

    @Override
    public void createIndex(EventLoop eventLoop,
            IndexListener indexListener,
            Policy policy,
            String s,
            String s1,
            String s2,
            String s3,
            IndexType indexType,
            IndexCollectionType indexCollectionType,
            CTX... ctxes) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Create complex secondary index to be used on bins containing collections.
     * This asynchronous server call will return before command is complete. The
     * user can optionally wait for command completion by using the returned
     * IndexTask instance.
     * <p>
     * This method is only supported by Aerospike 3 servers.
     *
     * @param policy              generic configuration parameters, pass in null for defaults
     * @param namespace           namespace - equivalent to database name
     * @param setName             optional set name - equivalent to database table
     * @param indexName           name of secondary index
     * @param binName             bin name that data is indexed on
     * @param indexType           underlying data type of secondary index
     * @param indexCollectionType index collection type
     * @throws AerospikeException if index create fails
     */
    public IndexTask createIndex(Policy policy, String namespace,
                                 String setName, String indexName, String binName,
                                 IndexType indexType, IndexCollectionType indexCollectionType)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "createIndex is not supported in MockAerospike");
    }

    /**
     * Delete secondary index. This method is only supported by Aerospike 3
     * servers.
     *
     * @param policy    generic configuration parameters, pass in null for defaults
     * @param namespace namespace - equivalent to database name
     * @param setName   optional set name - equivalent to database table
     * @param indexName name of secondary index
     * @throws AerospikeException if index create fails
     */
    public IndexTask dropIndex(Policy policy, String namespace, String setName,
                               String indexName) throws AerospikeException {
        throw new UnsupportedOperationException(
                "dropIndex is not supported in MockAerospike");
    }

    @Override
    public void dropIndex(EventLoop eventLoop,
            IndexListener indexListener,
            Policy policy,
            String s,
            String s1,
            String s2) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void info(EventLoop eventLoop,
            InfoListener infoListener,
            InfoPolicy infoPolicy,
            Node node,
            String... strings) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void setXDRFilter(InfoPolicy infoPolicy,
            String s,
            String s1,
            Expression expression) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Create user with password and roles. Clear-text password will be hashed
     * using bcrypt before sending to server.
     *
     * @param policy   admin configuration parameters, pass in null for defaults
     * @param user     user name
     * @param password user password in clear-text format
     * @param roles    variable arguments array of role names. Valid roles are listed
     *                 in Role.cs
     * @throws AerospikeException if command fails
     */
    public void createUser(AdminPolicy policy, String user, String password,
                           List<String> roles) throws AerospikeException {
        throw new UnsupportedOperationException(
                "createUser is not supported in MockAerospike");

    }

    /**
     * Remove user from cluster.
     *
     * @param policy admin configuration parameters, pass in null for defaults
     * @param user   user name
     * @throws AerospikeException if command fails
     */
    public void dropUser(AdminPolicy policy, String user)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "dropUser is not supported in MockAerospike");

    }

    /**
     * Change user's password. Clear-text password will be hashed using bcrypt
     * before sending to server.
     *
     * @param policy   admin configuration parameters, pass in null for defaults
     * @param user     user name
     * @param password user password in clear-text format
     * @throws AerospikeException if command fails
     */
    public void changePassword(AdminPolicy policy, String user, String password)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "changePassword is not supported in MockAerospike");

    }

    /**
     * Add roles to user's list of roles.
     *
     * @param policy admin configuration parameters, pass in null for defaults
     * @param user   user name
     * @param roles  role names. Valid roles are listed in Role.cs
     * @throws AerospikeException if command fails
     */
    public void grantRoles(AdminPolicy policy, String user, List<String> roles)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "grantRoles is not supported in MockAerospike");

    }

    /**
     * Remove roles from user's list of roles.
     *
     * @param policy admin configuration parameters, pass in null for defaults
     * @param user   user name
     * @param roles  role names. Valid roles are listed in Role.cs
     * @throws AerospikeException if command fails
     */
    public void revokeRoles(AdminPolicy policy, String user, List<String> roles)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "revokeRoles is not supported in MockAerospike");

    }

    /**
     * Create user defined role.
     *
     * @param policy     admin configuration parameters, pass in null for defaults
     * @param roleName   role name
     * @param privileges privileges assigned to the role.
     * @throws AerospikeException if command fails
     */
    public void createRole(AdminPolicy policy, String roleName,
                           List<Privilege> privileges) throws AerospikeException {
        throw new UnsupportedOperationException(
                "createRole is not supported in MockAerospike");

    }

    @Override
    public void createRole(AdminPolicy adminPolicy,
            String s,
            List<Privilege> list,
            List<String> list1) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void createRole(AdminPolicy adminPolicy,
            String s,
            List<Privilege> list,
            List<String> list1,
            int i,
            int i1) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Drop user defined role.
     *
     * @param policy   admin configuration parameters, pass in null for defaults
     * @param roleName role name
     * @throws AerospikeException if command fails
     */
    public void dropRole(AdminPolicy policy, String roleName)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "dropRole is not supported in MockAerospike");

    }

    /**
     * Grant privileges to an user defined role.
     *
     * @param policy     admin configuration parameters, pass in null for defaults
     * @param roleName   role name
     * @param privileges privileges assigned to the role.
     * @throws AerospikeException if command fails
     */
    public void grantPrivileges(AdminPolicy policy, String roleName,
                                List<Privilege> privileges) throws AerospikeException {
        throw new UnsupportedOperationException(
                "grantPrivileges is not supported in MockAerospike");

    }

    /**
     * Revoke privileges from an user defined role.
     *
     * @param policy     admin configuration parameters, pass in null for defaults
     * @param roleName   role name
     * @param privileges privileges assigned to the role.
     * @throws AerospikeException if command fails
     */
    public void revokePrivileges(AdminPolicy policy, String roleName,
                                 List<Privilege> privileges) throws AerospikeException {
        throw new UnsupportedOperationException(
                "revokePrivileges is not supported in MockAerospike");

    }

    @Override
    public void setWhitelist(AdminPolicy adminPolicy,
            String s,
            List<String> list) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void setQuotas(AdminPolicy adminPolicy,
            String s,
            int i,
            int i1) throws AerospikeException {
        // Do Nothing
    }

    /**
     * Retrieve roles for a given user.
     *
     * @param policy admin configuration parameters, pass in null for defaults
     * @param user   user name filter
     * @throws AerospikeException if command fails
     */
    public User queryUser(AdminPolicy policy, String user)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "queryUser is not supported in MockAerospike");
    }

    /**
     * Retrieve all users and their roles.
     *
     * @param policy admin configuration parameters, pass in null for defaults
     * @throws AerospikeException if command fails
     */
    public List<User> queryUsers(AdminPolicy policy) throws AerospikeException {
        throw new UnsupportedOperationException(
                "queryUsers is not supported in MockAerospike");
    }

    /**
     * Retrieve role definition.
     *
     * @param policy   admin configuration parameters, pass in null for defaults
     * @param roleName role name filter
     * @throws AerospikeException if command fails
     */
    public Role queryRole(AdminPolicy policy, String roleName)
            throws AerospikeException {
        throw new UnsupportedOperationException(
                "queryRole is not supported in MockAerospike");
    }

    /**
     * Retrieve all roles.
     *
     * @param policy admin configuration parameters, pass in null for defaults
     * @throws AerospikeException if command fails
     */
    public List<Role> queryRoles(AdminPolicy policy) throws AerospikeException {
        throw new UnsupportedOperationException(
                "queryRoles is not supported in MockAerospike");
    }

    /**
     * @param bins
     * @return
     */
    private Map<String, Object> convertToMap(Bin[] bins) {
        Map<String, Object> binMap = new HashMap<>(bins.length);
        for (Bin bin : bins) {
            if (bin.value instanceof Value.BooleanValue) {
                binMap.put(bin.name, bin.value.toLong());
            } else {
                binMap.put(bin.name, bin.value.getObject());
            }
        }
        return binMap;
    }

    @Override
    public Policy getReadPolicyDefault() {
        return new Policy();
    }

    @Override
    public WritePolicy getWritePolicyDefault() {
        return new WritePolicy();
    }

    @Override
    public ScanPolicy getScanPolicyDefault() {
        return new ScanPolicy();
    }

    @Override
    public QueryPolicy getQueryPolicyDefault() {
        return new QueryPolicy();
    }

    @Override
    public BatchPolicy getBatchPolicyDefault() {
        return new BatchPolicy();
    }

    @Override
    public BatchPolicy getBatchParentPolicyWriteDefault() {
        return null;
    }

    @Override
    public BatchWritePolicy getBatchWritePolicyDefault() {
        return null;
    }

    @Override
    public BatchDeletePolicy getBatchDeletePolicyDefault() {
        return null;
    }

    @Override
    public BatchUDFPolicy getBatchUDFPolicyDefault() {
        return null;
    }

    @Override
    public InfoPolicy getInfoPolicyDefault() {
        return new InfoPolicy();
    }

    @Override
    public boolean get(BatchPolicy policy, List<BatchRead> records)
            throws AerospikeException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
        // Do Nothing
    }

    @Override
    public RegisterTask register(Policy policy, ClassLoader resourceLoader,
                                 String resourcePath, String serverPath, Language language)
            throws AerospikeException {
        return null;
    }

    @Override
    public RegisterTask registerUdfString(Policy policy, String code, String serverPath, Language language) throws AerospikeException {
        return null;
    }

    @Override
    public void removeUdf(InfoPolicy policy, String serverPath)
            throws AerospikeException {

    }

    @Override
    public ResultSet queryAggregate(QueryPolicy policy, Statement statement)
            throws AerospikeException {
        return null;
    }

    @Override
    public ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
        return null;
    }

    private Record applyOperation(Operation operation, Key key) {
        switch (operation.type) {
            case ADD:
                Record magazineRecord = data.getOrDefault(key,
                        new Record(Collections.singletonMap(operation.binName, 0L), 0, 0));
                Map<String, Object> binMap = new HashMap<>(magazineRecord.bins);
                binMap.put(operation.binName, magazineRecord.getLong(operation.binName) + 1);
                data.put(key, new Record(binMap, 0 , 0));
            case READ:
                return data.getOrDefault(key, new Record(Collections.singletonMap(operation.binName, 0L), 0, 0));
            default:
                throw new UnsupportedOperationException(
                        String.format("%s operation is not supported in MockAerospike", operation.type));
        }
    }
}

