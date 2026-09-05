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

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

/**
 * Locks the layering that the 2.0.0 restructure established. These are the invariants that keep
 * storage internals - and third-party types, above all DLM's - out of the supported API.
 */
class ArchitectureTest {

    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("com.phonepe.magazine");

    /**
     * DLM drags in hbase-shaded-client, log4j 1.2.17 and junit 4.12, all of which magazine-core
     * excludes wholesale. Confining DLM types to the deduplication guard keeps that exclusion safe
     * and keeps the blast radius of the eventual dlm-aerospike migration to one class.
     */
    @Test
    void dlmTypesAreConfinedToTheDeduplicationGuard() {
        noClasses()
                .that().haveNameNotMatching(".*DeDupeGuard.*")
                .and().resideOutsideOfPackage("com.phonepe.magazine.impl.aerospike.common")
                .should().dependOnClassesThat().resideInAnyPackage("com.phonepe.dlm..")
                .because("DLM's transitive graph is excluded wholesale; its types must not leak")
                .check(CLASSES);
    }

    /** Aerospike is an implementation detail; the API and SPI must not name its types. */
    @Test
    void aerospikeTypesStayInsideTheAerospikeImplementation() {
        noClasses()
                .that().resideOutsideOfPackage("com.phonepe.magazine.impl..")
                .should().dependOnClassesThat().resideInAnyPackage("com.aerospike..")
                .because("storage internals must not appear in the API, entities or the SPI")
                .check(CLASSES);
    }

    /** Entities are storage-agnostic values; they must not reach up into the API or an impl. */
    @Test
    void entitiesDependOnNothingButExceptions() {
        noClasses()
                .that().resideInAPackage("com.phonepe.magazine.entity")
                .should().dependOnClassesThat()
                .resideInAnyPackage("com.phonepe.magazine", "com.phonepe.magazine.core",
                        "com.phonepe.magazine.impl..")
                .because("entities sit at the bottom of the layering")
                .check(CLASSES);
    }

    /** The SPI must not know its implementations. */
    @Test
    void theStorageSpiDoesNotDependOnAnyImplementation() {
        noClasses()
                .that().resideInAPackage("com.phonepe.magazine.core")
                .should().dependOnClassesThat().resideInAnyPackage("com.phonepe.magazine.impl..")
                .because("BaseMagazineStorage is implemented by backends, not aware of them")
                .check(CLASSES);
    }

    /** Caught a real cycle during the restructure: core -> MagazineContext -> core. */
    @Test
    void packagesAreFreeOfCycles() {
        final ArchRule rule = slices()
                .matching("com.phonepe.magazine.(**)")
                .should().beFreeOfCycles();
        rule.check(CLASSES);
    }
}
