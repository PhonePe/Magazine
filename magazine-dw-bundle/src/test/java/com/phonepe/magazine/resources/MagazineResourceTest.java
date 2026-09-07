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
import com.phonepe.magazine.entity.MagazineData;
import com.phonepe.magazine.entity.MetaData;
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
        final Magazine<Object> opaque = Mockito.mock(Magazine.class);
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
                PeekedData.class)) {
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
        return Mockito.mock(Magazine.class);
    }
}
