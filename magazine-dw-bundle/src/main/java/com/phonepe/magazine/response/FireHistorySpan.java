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

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.Instant;

/**
 * How far back the retained history actually reaches.
 * <p>
 * The number an operator needs before trusting anything built on this: a redrive asking about a
 * moment older than {@link #oldest()} gets an error rather than an answer. Reported from the data
 * rather than from {@code window x entries}, because eviction is by count and gaps stretch the real
 * span well beyond the nominal one.
 */
public record FireHistorySpan(@JsonFormat(shape = JsonFormat.Shape.STRING) Instant oldest,
                              @JsonFormat(shape = JsonFormat.Shape.STRING) Instant newest,
                              long spanSeconds,
                              int windows,
                              int shardsReporting,
                              int shardsConfigured) {
}
