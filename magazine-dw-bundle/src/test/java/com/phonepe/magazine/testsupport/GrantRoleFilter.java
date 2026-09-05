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

package com.phonepe.magazine.testsupport;

import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import lombok.RequiredArgsConstructor;

/**
 * Stands in for the application's authentication, granting a single role.
 * <p>
 * Must run at {@link Priorities#AUTHENTICATION}: {@code RolesAllowedDynamicFeature} evaluates at
 * {@link Priorities#AUTHORIZATION}, so a filter left at the default user priority would populate
 * the {@link SecurityContext} only after the role check had already failed.
 */
@Priority(Priorities.AUTHENTICATION)
@RequiredArgsConstructor
public final class GrantRoleFilter implements ContainerRequestFilter {

    private final String role;

    @Override
    public void filter(final ContainerRequestContext requestContext) {
        requestContext.setSecurityContext(new SecurityContext() {
            @Override
            public Principal getUserPrincipal() {
                return () -> "test-operator";
            }

            @Override
            public boolean isUserInRole(final String candidate) {
                return role.equals(candidate);
            }

            @Override
            public boolean isSecure() {
                return false;
            }

            @Override
            public String getAuthenticationScheme() {
                return "TEST";
            }
        });
    }
}
