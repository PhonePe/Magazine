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

package com.phonepe.magazine.response;

/**
 * A single peeked record.
 *
 * @param shard   the shard it was read from, or null for an unsharded magazine.
 * @param pointer the fire pointer it sits at.
 * @param type    simple class name of the payload, so the dashboard can render it sensibly.
 * @param data    the payload, converted to a JSON-safe representation; falls back to the payload's
 *                {@code toString()} when it cannot be serialised.
 */
public record PeekedData(Integer shard, long pointer, String type, Object data) {
}
