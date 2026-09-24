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

import com.phonepe.magazine.core.BaseMagazineStorage;
import com.phonepe.magazine.entity.FireCheckpoint;
import com.phonepe.magazine.entity.MagazineContext;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MagazineScope;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.entity.StorageType;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MagazineManagerTest {

    private MagazineManager manager;
    private AtomicInteger initializations;

    @BeforeEach
    void setup() {
        manager = new MagazineManager("CLIENT_ID");
        initializations = new AtomicInteger();
    }

    @Test
    @DisplayName("replaceAll evicts everything absent from the list")
    void replaceAllEvicts() {
        manager.replaceAll(List.of(magazine("A"), magazine("B")));
        manager.replaceAll(List.of(magazine("C")));

        assertEquals(Set.of("C"), manager.getMagazineMap().keySet());
        assertThrows(MagazineException.class, () -> manager.getMagazine("A"));
    }

    @Test
    @DisplayName("register adds without disturbing anything already registered")
    void registerIsAdditive() {
        manager.replaceAll(List.of(magazine("A"), magazine("B")));
        manager.register(magazine("C"));

        assertEquals(Set.of("A", "B", "C"), manager.getMagazineMap().keySet());
    }

    @Test
    @DisplayName("registering the same identifier twice swaps the handle rather than duplicating it")
    void registerReplacesSameIdentifier() {
        manager.register(magazine("A"));
        final Magazine<String> replacement = magazine("A");
        manager.register(replacement);

        assertEquals(1, manager.getMagazineMap().size());
        assertSame(replacement, manager.getMagazine("A"));
    }

    @Test
    @DisplayName("getOrRegister does not build a magazine that is already registered")
    void getOrRegisterSkipsTheFactory() {
        final Magazine<String> first = manager.getOrRegister("A", () -> magazine("A"));
        assertEquals(1, initializations.get(), "the first call must build");

        final Magazine<String> second = manager.getOrRegister("A", () -> magazine("A"));
        assertSame(first, second);
        assertEquals(1, initializations.get(),
                "a registered magazine must not be rebuilt; building costs a shard-config read");
    }

    @Test
    @DisplayName("getOrRegister rejects a factory that returns a differently identified magazine")
    void getOrRegisterValidatesTheFactoryResult() {
        assertEquals(ErrorCode.INVALID_CONFIGURATION,
                assertThrows(MagazineException.class,
                        () -> manager.getOrRegister("A", () -> magazine("B"))).getErrorCode());
        assertTrue(manager.getMagazineMap().isEmpty(), "a rejected factory must register nothing");
    }

    @Test
    @DisplayName("find reports absence without throwing, and getMagazine still throws")
    void findDoesNotThrowOnAbsence() {
        manager.register(magazine("A"));

        assertSame(manager.getMagazine("A"), manager.find("A").orElseThrow());
        assertTrue(manager.find("MISSING").isEmpty());
        assertTrue(manager.find(null).isEmpty(), "a null identifier is absent, not an error");
        assertTrue(manager.find("  ").isEmpty());

        assertEquals(ErrorCode.MAGAZINE_NOT_FOUND,
                assertThrows(MagazineException.class, () -> manager.getMagazine("MISSING")).getErrorCode());
    }

    @Test
    @DisplayName("find lets a caller own the miss path without building on a hit")
    void findSupportsCallerOwnedMissPath() {
        for (int attempt = 0; attempt < 3; attempt++) {
            manager.<String>find("A").orElseGet(() -> {
                final Magazine<String> built = magazine("A");
                manager.register(built);
                return built;
            });
        }
        assertEquals(1, initializations.get(), "only the first miss may build");
    }

    @Test
    @DisplayName("unregister drops one magazine and reports whether it was there")
    void unregisterDropsOne() {
        manager.replaceAll(List.of(magazine("A"), magazine("B")));

        assertTrue(manager.unregister("A"));
        assertFalse(manager.unregister("A"));
        assertEquals(Set.of("B"), manager.getMagazineMap().keySet());
    }

    @Test
    @DisplayName("concurrent registrations all survive")
    void concurrentRegistrationsDoNotClobberEachOther() throws Exception {
        final int count = 32;
        runConcurrently(count, index -> manager.register(magazine("M" + index)));

        assertEquals(count, manager.getMagazineMap().size(),
                "a get-then-set registry would lose updates here");
    }

    @Test
    @DisplayName("concurrent getOrRegister on one identifier hands every caller the same handle")
    void concurrentGetOrRegisterConvergesOnOneHandle() throws Exception {
        final int threads = 16;
        final Magazine<?>[] seen = new Magazine<?>[threads];
        runConcurrently(threads, index -> seen[index] = manager.getOrRegister("A", () -> magazine("A")));

        final Magazine<?> registered = manager.getMagazine("A");
        for (int i = 0; i < threads; i++) {
            assertSame(registered, seen[i],
                    "every caller must receive the registration that won, not its own losing build");
        }
    }

    // --- helpers ---------------------------------------------------------------------------

    private interface IndexedAction {
        void run(int index);
    }

    private void runConcurrently(final int count, final IndexedAction action) throws Exception {
        final ExecutorService pool = Executors.newFixedThreadPool(Math.min(count, 8));
        final CountDownLatch start = new CountDownLatch(1);
        try {
            final List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < count; i++) {
                final int index = i;
                futures.add(pool.submit(() -> {
                    start.await();
                    action.run(index);
                    return null;
                }));
            }
            start.countDown();
            for (java.util.concurrent.Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private Magazine<String> magazine(final String identifier) {
        return Magazine.<String>builder()
                .magazineIdentifier(identifier)
                .baseMagazineStorage(new CountingStorage(initializations))
                .build();
    }

    /**
     * Counts {@code initialize} calls, which is what a rebuild costs in the real backend.
     */
    private static final class CountingStorage extends BaseMagazineStorage<String> {

        private final AtomicInteger initializations;

        private CountingStorage(final AtomicInteger initializations) {
            super(StorageType.AEROSPIKE, 60, 120, "FARM_ID", false, "CLIENT_ID", MagazineScope.LOCAL);
            this.initializations = initializations;
        }

        @Override
        public MagazineContext initialize(final String magazineIdentifier) {
            initializations.incrementAndGet();
            return new MagazineContext(magazineIdentifier, 2, 1);
        }

        @Override
        public boolean load(final MagazineContext context, final String data) {
            return true;
        }

        @Override
        public boolean reload(final MagazineContext context, final String data) {
            return true;
        }

        @Override
        public MagazineData<String> fire(final MagazineContext context) {
            return null;
        }

        @Override
        public Map<String, MetaData> getMetaData(final MagazineContext context) {
            return Map.of();
        }

        @Override
        public void delete(final MagazineContext context, final MagazineData<String> magazineData) {
            // no-op
        }

        @Override
        public void deleteAll(final MagazineContext context, final Collection<MagazineData<String>> magazineData) {
            // no-op
        }

        @Override
        public Set<MagazineData<String>> peek(final MagazineContext context,
                final Map<Integer, Set<Long>> shardPointersMap) {
            return Set.of();
        }

        @Override
        public Map<String, FireCheckpoint> firePointerBefore(final MagazineContext context, final Instant instant) {
            return Map.of();
        }

        @Override
        public Map<String, List<FireCheckpoint>> fireHistory(final MagazineContext context) {
            return Map.of();
        }
    }
}
