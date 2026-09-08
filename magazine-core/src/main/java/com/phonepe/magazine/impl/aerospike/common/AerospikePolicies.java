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

package com.phonepe.magazine.impl.aerospike.common;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import java.util.Objects;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Write policies are resolved once per store and then treated as read-only, which is how the
 * Aerospike client treats its own shared defaults.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AerospikePolicies {

    /**
     * @return a copy of the client's default write policy, or a fresh default when it exposes
     *         none. {@link IAerospikeClient} is an interface, so a double or wrapper may return
     *         null, and an NPE from inside a copy constructor is a poor way to report that.
     */
    public static WritePolicy writePolicy(final IAerospikeClient client) {
        final WritePolicy defaults = client.getWritePolicyDefault();
        return Objects.isNull(defaults) ? new WritePolicy() : new WritePolicy(defaults);
    }
}
