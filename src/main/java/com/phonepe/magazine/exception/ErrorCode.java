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
package com.phonepe.magazine.exception;

public enum ErrorCode {
    CONNECTION_ERROR,
    INTERNAL_ERROR,
    RETRIES_EXHAUSTED,
    MAGAZINE_UNPREPARED,
    NOTHING_TO_FIRE,
    MAGAZINE_NOT_FOUND,
    ACTION_DENIED_PARALLEL_ATTEMPT,
    NOT_IMPLEMENTED,
    INVALID_SHARDS,
    DATA_TYPE_MISMATCH
}
