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

package com.phonepe.magazine;

import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import com.phonepe.magazine.exception.MagazineExceptions;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Registry of the {@link Magazine} handles a client has opened.
 */
@Getter
@EqualsAndHashCode(of = "clientId")
@ToString(of = "clientId")
public class MagazineManager {

    private final String clientId;
    @Getter(AccessLevel.NONE)
    private final AtomicReference<Map<String, Magazine<?>>> magazineMap = new AtomicReference<>(Map.of());

    public MagazineManager(final String clientId) {
        this.clientId = clientId;
    }

    /**
     * Replaces the entire registry with the magazines provided.
     * <p>
     * Every magazine not in {@code magazines} is evicted. Calling this with a single magazine
     * therefore unregisters all the others, and the next access to one of them fails with
     * {@link ErrorCode#MAGAZINE_NOT_FOUND} until it is rebuilt. Use {@link #register(Magazine)} to
     * add one magazine.
     *
     * @param magazines the complete set of magazines this manager should serve.
     */
    public void replaceAll(final List<Magazine<?>> magazines) {
        magazineMap.set(magazines.stream()
                .collect(Collectors.toUnmodifiableMap(
                        Magazine::getMagazineIdentifier,
                        Function.identity())));
    }

    /**
     * Adds a magazine, leaving every other registration untouched, and replacing any existing
     * registration under the same identifier.
     *
     * @param magazine the magazine to register.
     */
    public void register(final Magazine<?> magazine) {
        if (Objects.isNull(magazine)) {
            throw MagazineExceptions.invalidConfiguration("Magazine is required.");
        }
        magazineMap.updateAndGet(current -> copyWith(current, magazine));
    }

    /**
     * Returns the magazine registered under {@code magazineIdentifier}, building and registering
     * one through {@code factory} only if none exists.
     *
     * @param magazineIdentifier identifier to look up.
     * @param factory            builds the magazine when it is absent. Must not return null.
     * @return the registered magazine.
     */
    @SuppressWarnings("unchecked")
    public <T> Magazine<T> getOrRegister(final String magazineIdentifier,
            final Supplier<Magazine<T>> factory) {
        if (Objects.isNull(magazineIdentifier) || magazineIdentifier.isBlank()) {
            throw MagazineExceptions.invalidConfiguration("Magazine identifier is required.");
        }
        if (Objects.isNull(factory)) {
            throw MagazineExceptions.invalidConfiguration("Magazine factory is required.");
        }
        final Optional<Magazine<T>> existing = find(magazineIdentifier);
        if (existing.isPresent()) {
            return existing.get();
        }
        final Magazine<T> built = factory.get();
        if (Objects.isNull(built)) {
            throw MagazineExceptions.invalidConfiguration(
                    String.format("Magazine factory returned null for identifier %s.", magazineIdentifier));
        }
        if (!magazineIdentifier.equals(built.getMagazineIdentifier())) {
            throw MagazineExceptions.invalidConfiguration(String.format(
                    "Magazine factory for identifier %s returned a magazine identified as %s.",
                    magazineIdentifier, built.getMagazineIdentifier()));
        }
        // Only the first writer wins, so concurrent builders converge on one handle rather than
        // each overwriting the last.
        return (Magazine<T>) magazineMap.updateAndGet(current -> current.containsKey(magazineIdentifier)
                        ? current
                        : copyWith(current, built))
                .get(magazineIdentifier);
    }

    /**
     * Removes a magazine from the registry. The underlying storage is untouched.
     *
     * @param magazineIdentifier identifier to drop.
     * @return true if a registration was removed.
     */
    public boolean unregister(final String magazineIdentifier) {
        if (Objects.isNull(magazineIdentifier) || !magazineMap.get().containsKey(magazineIdentifier)) {
            return false;
        }
        final Map<String, Magazine<?>> previous = magazineMap.getAndUpdate(current -> {
            final Map<String, Magazine<?>> updated = new HashMap<>(current);
            updated.remove(magazineIdentifier);
            return Map.copyOf(updated);
        });
        return previous.containsKey(magazineIdentifier);
    }

    public Map<String, Magazine<?>> getMagazineMap() {
        return magazineMap.get();
    }

    /**
     *  function provides magazine corresponding to magazine identifier
     *
     * @param magazineIdentifier
     * @return magazine corresponding to provided magazineIdentifier
     * @param <T>
     */
    public <T> Magazine<T> getMagazine(final String magazineIdentifier) {
        return this.<T>find(magazineIdentifier)
                .orElseThrow(() -> MagazineException.builder()
                        .message(String.format("Magazine not found for identifier %s", magazineIdentifier))
                        .errorCode(ErrorCode.MAGAZINE_NOT_FOUND)
                        .build());
    }

    /**
     * Looks up a magazine without treating absence as an error.
     *
     * @param magazineIdentifier identifier to look up. A null or blank identifier is simply absent.
     * @return the registered magazine, or empty.
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<Magazine<T>> find(final String magazineIdentifier) {
        if (Objects.isNull(magazineIdentifier) || magazineIdentifier.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable((Magazine<T>) magazineMap.get().get(magazineIdentifier));
    }

    private static Map<String, Magazine<?>> copyWith(final Map<String, Magazine<?>> current,
                                                     final Magazine<?> magazine) {
        final Map<String, Magazine<?>> updated = new HashMap<>(current);
        updated.put(magazine.getMagazineIdentifier(), magazine);
        return Map.copyOf(updated);
    }
}
