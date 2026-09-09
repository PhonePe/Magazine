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

package com.phonepe.magazine.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.magazine.Magazine;
import com.phonepe.magazine.MagazineManager;
import com.phonepe.magazine.entity.FireCheckpoint;
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MetaData;
import com.phonepe.magazine.exception.ErrorCode;
import com.phonepe.magazine.exception.MagazineExceptions;
import com.phonepe.magazine.request.PeekRequest;
import com.phonepe.magazine.response.*;
import com.phonepe.magazine.service.MagazineService;
import com.phonepe.magazine.testsupport.GrantRoleFilter;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(DropwizardExtensionsSupport.class)
class MagazineResourceTest {

    private static final Instant OLDEST = Instant.parse("2026-01-01T10:00:00Z");
    private static final Instant MIDDLE = Instant.parse("2026-01-01T10:05:00Z");
    private static final Instant NEWEST = Instant.parse("2026-01-01T10:10:00Z");

    private final Magazine<String> magazine = mockMagazine();
    private final MagazineManager magazineManager = mock(MagazineManager.class);
    private final MagazineService service = new MagazineService(magazineManager, new ObjectMapper());
    private final ResourceExtension resources = ResourceExtension.builder()
            .addResource(new MagazineResource(service))
            .addProvider(RolesAllowedDynamicFeature.class)
            .addProvider(new GrantRoleFilter(MagazineResource.PEEK_ROLE))
            .build();

    @BeforeEach
    void resetMagazine() {
        Mockito.reset(magazine);
        when(magazine.getMagazineIdentifier()).thenReturn("jobs");
        when(magazine.getShards()).thenReturn(4);
        when(magazineManager.getMagazineMap()).thenReturn(Map.of("jobs", magazine));
    }

    @Test
    void listsOnlyConfiguredMagazines() {
        final List<MagazineDescriptor> response = resources.target("/magazine/v1/magazines")
                .request()
                .get(new GenericType<>() {
                });

        assertEquals(List.of(new MagazineDescriptor("jobs")), response);
        verifyNoDataOperations();
    }

    private void stubHistory() {
        // SHARD_0 missed the oldest window - it may not have existed yet.
        when(magazine.fireHistory()).thenReturn(Map.of(
                "SHARD_1", List.of(new FireCheckpoint(512, NEWEST), new FireCheckpoint(500, MIDDLE),
                        new FireCheckpoint(400, OLDEST)),
                "SHARD_0", List.of(new FireCheckpoint(340, NEWEST), new FireCheckpoint(300, MIDDLE))));
    }

    private FireHistoryResponse history(final String shard) {
        var target = resources.target("/magazine/v1/magazines/jobs/fire-history");
        if (shard != null) {
            target = target.queryParam("shard", shard);
        }
        return target.request().get(FireHistoryResponse.class);
    }

    @Test
    void reportsRetainedHistoryAsOneRowPerWindowNewestFirst() {
        stubHistory();

        final FireHistoryResponse response = history(null);

        assertTrue(response.enabled());
        assertNull(response.shard(), "totals by default");
        assertEquals(List.of("SHARD_0", "SHARD_1"), response.shards());
        assertEquals(List.of(NEWEST, MIDDLE, OLDEST),
                response.windows().stream().map(FireHistoryWindow::recordedAt).toList());
        assertEquals(852, response.windows().get(0).firePointer(), "totalled across reporting shards");
        assertEquals(2, response.windows().get(0).shardsReporting());
        assertEquals(1, response.windows().get(2).shardsReporting());
        verifyNoDataOperations();
    }

    /**
     * The row count must not grow with shard count - a magazine may have hundreds of shards, and
     * the payload is polled every 30 seconds.
     */
    @Test
    void rowCountIsIndependentOfShardCount() {
        final Instant window = Instant.parse("2026-01-01T10:00:00Z");
        final Map<String, List<FireCheckpoint>> wide = new java.util.LinkedHashMap<>();
        for (int shard = 0; shard < 256; shard++) {
            wide.put("SHARD_" + shard, List.of(new FireCheckpoint(10, window)));
        }
        when(magazine.fireHistory()).thenReturn(wide);
        when(magazine.getShards()).thenReturn(256);

        final FireHistoryResponse response = history(null);

        assertEquals(1, response.windows().size(), "one row per window, whatever the shard count");
        assertEquals(2560, response.windows().get(0).firePointer());
        assertEquals(256, response.span().shardsReporting());
        assertEquals(256, response.span().shardsConfigured());
    }

    /**
     * A shard that only appears in the newer window must not report its whole pointer as having
     * been delivered in that window.
     */
    @Test
    void deliveredCountsOnlyShardsPresentInBothWindows() {
        stubHistory();

        final List<FireHistoryWindow> windows = history(null).windows();

        assertNull(windows.get(2).delivered(), "the oldest row has nothing to compare against");
        assertEquals(100, windows.get(1).delivered(), "only SHARD_1 spans both, 500 - 400");
        assertEquals(52, windows.get(0).delivered(), "both shards span, (512-500) + (340-300)");
    }

    @Test
    void reportsHowFarBackHistoryReaches() {
        stubHistory();

        final FireHistorySpan span = history(null).span();

        assertEquals(OLDEST, span.oldest());
        assertEquals(NEWEST, span.newest());
        assertEquals(3, span.windows());
        assertEquals(2, span.shardsReporting());
        assertEquals(4, span.shardsConfigured());
    }

    @Test
    void drillsIntoASingleShard() {
        stubHistory();

        final FireHistoryResponse response = history("SHARD_0");

        assertEquals("SHARD_0", response.shard());
        assertEquals(List.of(NEWEST, MIDDLE),
                response.windows().stream().map(FireHistoryWindow::recordedAt).toList(),
                "only the windows that shard recorded");
        assertEquals(340, response.windows().get(0).firePointer(), "that shard's pointer, not a total");
        assertEquals(List.of("SHARD_0", "SHARD_1"), response.shards(), "the selector still lists every shard");
    }

    @Test
    void rejectsAShardWithNoRecordedHistory() {
        stubHistory();

        try (Response response = resources.target("/magazine/v1/magazines/jobs/fire-history")
                .queryParam("shard", "SHARD_3")
                .request()
                .get()) {
            assertEquals(400, response.getStatus());
        }
    }

    /**
     * A magazine that does not record checkpoints is a configuration fact, not a failure. Surfacing
     * it as a 500 would make an unconfigured dashboard look like a broken one.
     */
    @Test
    void reportsFireHistoryAsDisabledRatherThanFailing() {
        when(magazine.fireHistory()).thenThrow(MagazineExceptions.of(ErrorCode.NOT_ENABLED, "not enabled"));

        final FireHistoryResponse response = history(null);

        assertFalse(response.enabled());
        assertTrue(response.windows().isEmpty());
        assertNull(response.span());
    }

    /**
     * Browsing history must never write to the magazine being inspected. firePointerBefore records
     * a checkpoint as a side effect; fireHistory deliberately does not, and the console uses that.
     */
    @Test
    void browsingHistoryDoesNotRecordAnything() {
        when(magazine.fireHistory()).thenReturn(Map.of());

        assertNull(history(null).span());

        verify(magazine, never()).firePointerBefore(any());
        verifyNoDataOperations();
    }

    @Test
    void reflectsMagazineManagerRefreshes() {
        final Magazine<String> replacement = mockMagazine();
        when(replacement.getMagazineIdentifier()).thenReturn("replacement");
        when(replacement.getShards()).thenReturn(2);
        when(magazineManager.getMagazineMap())
                .thenReturn(Map.of("jobs", magazine))
                .thenReturn(Map.of("replacement", replacement));

        assertEquals(List.of(new MagazineDescriptor("jobs")), service.descriptors());
        assertEquals(List.of(new MagazineDescriptor("replacement")), service.descriptors());
    }

    @Test
    void returnsMetadataWithoutCallingDataOperations() {
        when(magazine.getMetaData()).thenReturn(Map.of(
                "SHARD_1", new MetaData(2, 5, 3, 7),
                "SHARD_0", new MetaData(1, 4, 2, 6)));

        final MagazineMetadataResponse response = resources
                .target("/magazine/v1/magazines/jobs/metadata")
                .request()
                .get(MagazineMetadataResponse.class);

        assertEquals("jobs", response.identifier());
        assertEquals(Set.of("SHARD_0", "SHARD_1"), response.shards().keySet());
        assertEquals(new ShardMetadata(9, 3, 0, 0, 6), response.totals());
        verify(magazine).getMetaData();
        verify(magazine, never()).peek(any());
        verifyNoMutationOperations();
    }

    @Test
    void ordersShardsNumerically() {
        when(magazine.getMetaData()).thenReturn(Map.of(
                "SHARD_10", new MetaData(0, Long.MAX_VALUE, 0, Long.MAX_VALUE),
                "SHARD_2", new MetaData(0, Long.MAX_VALUE, 0, Long.MAX_VALUE)));

        final MagazineMetadataResponse response = resources
                .target("/magazine/v1/magazines/jobs/metadata")
                .request()
                .get(MagazineMetadataResponse.class);

        assertEquals(List.of("SHARD_2", "SHARD_10"), response.shards().keySet().stream().toList());
    }

    @Test
    void peeksWithoutReadingOrChangingMetadata() {
        final Map<Integer, Set<Long>> pointers = Map.of(0, Set.of(1L, 2L));
        when(magazine.peek(pointers)).thenReturn(Set.of(
                new MagazineData<>("second", 2, 0, "jobs"),
                new MagazineData<>("first", 1, 0, "jobs")));

        final PeekResponse response = resources.target("/magazine/v1/magazines/jobs/peek")
                .request()
                .post(Entity.json(new PeekRequest(pointers)), PeekResponse.class);

        assertEquals(List.of(
                new PeekedData(0, 1, "String", "first"),
                new PeekedData(0, 2, "String", "second")), response.data());
        verify(magazine).peek(pointers);
        verify(magazine, never()).getMetaData();
        verifyNoMutationOperations();
    }

    @Test
    void rejectsOversizedPeekBeforeCallingMagazine() {
        final Set<Long> pointers = LongStream.rangeClosed(1, 1_001)
                .boxed()
                .collect(Collectors.toSet());
        final PeekRequest request = new PeekRequest(Map.of(0, pointers));

        try (Response response = resources.target("/magazine/v1/magazines/jobs/peek")
                .request()
                .post(Entity.json(request))) {
            assertEquals(400, response.getStatus());
        }
        verify(magazine, never()).peek(any());
        verifyNoMutationOperations();
    }

    @Test
    void rejectsOutOfRangeShardBeforeCallingMagazine() {
        final PeekRequest request = new PeekRequest(Map.of(4, Set.of(1L)));

        try (Response response = resources.target("/magazine/v1/magazines/jobs/peek")
                .request()
                .post(Entity.json(request))) {
            assertEquals(400, response.getStatus());
        }
        verify(magazine, never()).peek(any());
        verifyNoMutationOperations();
    }

    @Test
    void translatesShardZeroForUnshardedMagazine() {
        final Magazine<String> unsharded = mockMagazine();
        when(unsharded.getMagazineIdentifier()).thenReturn("single");
        when(unsharded.getShards()).thenReturn(1);
        final Map<Integer, Set<Long>> normalized = Collections.singletonMap(null, Set.of(1L));
        when(unsharded.peek(normalized)).thenReturn(Set.of(
                new MagazineData<>("payload", 1, null, "single")));
        final MagazineManager unshardedManager = mock(MagazineManager.class);
        when(unshardedManager.getMagazineMap()).thenReturn(Map.of("single", unsharded));
        final MagazineService unshardedService = new MagazineService(unshardedManager, new ObjectMapper());

        final PeekResponse response = unshardedService.peek(
                "single", new PeekRequest(Map.of(0, Set.of(1L))));

        assertEquals(List.of(new PeekedData(null, 1, "String", "payload")), response.data());
        verify(unsharded).peek(normalized);
        verify(unsharded, never()).fire();
    }

    @Test
    void peekIsRejectedWithoutThePeekRole() throws Throwable {
        // No SecurityContext is populated, so isUserInRole is false and Jersey denies. This is
        // what makes peek closed by default for an application that registers no authentication.
        final ResourceExtension unguarded = ResourceExtension.builder()
                .addResource(new MagazineResource(service))
                .addProvider(RolesAllowedDynamicFeature.class)
                .build();
        unguarded.before();
        try (Response response = unguarded.target("/magazine/v1/magazines/jobs/peek")
                .request()
                .post(Entity.json(new PeekRequest(Map.of(0, Set.of(1L)))))) {
            assertEquals(403, response.getStatus());
        } finally {
            unguarded.after();
        }
        verify(magazine, never()).peek(any());
    }

    @Test
    void countersAndPointersSerialiseAsJsonNumbers() {
        when(magazine.getMetaData()).thenReturn(Map.of("SHARD_0", new MetaData(1, 4, 2, 6)));

        final String body = resources.target("/magazine/v1/magazines/jobs/metadata")
                .request()
                .get(String.class);

        // Plain numbers, not quoted strings - the ToStringSerializer guarded against a 2^53
        // overflow that per-shard message counters cannot reach.
        assertTrue(body.contains("\"loadCounter\":4"), body);
        assertFalse(body.contains("\"loadCounter\":\"4\""), body);
    }

    @Test
    void unserialisablePayloadDegradesInsteadOfFailingTheRequest() {
        final Map<Integer, Set<Long>> pointers = Map.of(0, Set.of(1L));
        final Object hostile = new Object() {
            @Override
            public String toString() {
                return "opaque-payload";
            }
        };
        final MagazineManager manager = mock(MagazineManager.class);
        final Magazine<Object> opaque = mock(Magazine.class);
        when(opaque.getMagazineIdentifier()).thenReturn("jobs");
        when(opaque.getShards()).thenReturn(4);
        when(opaque.peek(pointers)).thenReturn(Set.of(new MagazineData<>(hostile, 1, 0, "jobs")));
        when(manager.getMagazineMap()).thenReturn(Map.of("jobs", opaque));
        final ObjectMapper strict = new ObjectMapper();
        strict.configure(com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS, true);

        final PeekResponse response = new MagazineService(manager, strict)
                .peek("jobs", new PeekRequest(pointers));

        assertEquals(1, response.data().size());
        assertEquals("opaque-payload", response.data().get(0).data());
    }

    @Test
    void mutatingRoutesAndMethodsAreAbsent() {
        assertStatus(404, "POST", "/magazine/v1/magazines/jobs/load");
        assertStatus(404, "POST", "/magazine/v1/magazines/jobs/reload");
        assertStatus(404, "POST", "/magazine/v1/magazines/jobs/fire");
        assertStatus(404, "DELETE", "/magazine/v1/magazines/jobs/1");
        assertStatus(405, "POST", "/magazine/v1/magazines/jobs/metadata");
        assertStatus(405, "DELETE", "/magazine/v1/magazines/jobs/peek");
        verifyNoDataOperations();
    }

    @Test
    void webApiDoesNotExposeStorageOrMutationMethods() {
        final Set<String> methods = Set.of("load", "reload", "fire", "delete");
        for (Class<?> type : List.of(
                MagazineResource.class,
                MagazineService.class,
                MagazineDescriptor.class,
                MagazineMetadataResponse.class,
                ShardMetadata.class,
                PeekRequest.class,
                PeekResponse.class,
                PeekedData.class,
                FireHistoryResponse.class,
                FireHistoryWindow.class,
                FireHistorySpan.class)) {
            for (Method method : type.getDeclaredMethods()) {
                assertFalse(methods.contains(method.getName()), type + " exposes " + method.getName());
                assertFalse(method.getReturnType().getName().contains("BaseMagazineStorage"));
                for (Class<?> parameter : method.getParameterTypes()) {
                    assertFalse(parameter.getName().contains("BaseMagazineStorage"));
                }
            }
        }
    }

    private void assertStatus(final int expected, final String method, final String path) {
        final Response response = "DELETE".equals(method)
                ? resources.target(path).request().method(method)
                : resources.target(path).request().method(
                method, Entity.entity("{}", MediaType.APPLICATION_JSON_TYPE));
        try (response) {
            assertEquals(expected, response.getStatus(), method + " " + path);
        }
    }

    private void verifyNoDataOperations() {
        verify(magazine, never()).getMetaData();
        verify(magazine, never()).peek(any());
        verifyNoMutationOperations();
    }

    private void verifyNoMutationOperations() {
        verify(magazine, never()).load(any());
        verify(magazine, never()).reload(any());
        verify(magazine, never()).fire();
        verify(magazine, never()).delete(any());
    }

    @SuppressWarnings("unchecked")
    private static Magazine<String> mockMagazine() {
        return mock(Magazine.class);
    }
}
