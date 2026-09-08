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

import com.phonepe.magazine.request.PeekRequest;
import com.phonepe.magazine.response.MagazineDescriptor;
import com.phonepe.magazine.response.MagazineMetadataResponse;
import com.phonepe.magazine.response.PeekResponse;
import com.phonepe.magazine.service.MagazineService;
import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import java.util.List;
import lombok.RequiredArgsConstructor;

@Path("/magazine/v1/magazines")
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor
public final class MagazineResource {

    /**
     * Role required to peek. Grant it from the application's own {@code Authorizer}; the bundle
     * intentionally defines no authentication of its own.
     */
    public static final String PEEK_ROLE = "magazine_peek";

    private final MagazineService service;

    @GET
    public List<MagazineDescriptor> magazines() {
        return service.descriptors();
    }

    @GET
    @Path("/{identifier}/metadata")
    public MagazineMetadataResponse metadata(@PathParam("identifier") final String identifier) {
        return service.metadata(identifier);
    }

    @POST
    @Path("/{identifier}/peek")
    @Consumes(MediaType.APPLICATION_JSON)
    @RolesAllowed(PEEK_ROLE)
    public PeekResponse peek(@PathParam("identifier") final String identifier,
            final PeekRequest request) {
        return service.peek(identifier, request);
    }
}
