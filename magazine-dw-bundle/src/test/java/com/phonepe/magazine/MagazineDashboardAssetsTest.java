package com.phonepe.magazine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class MagazineDashboardAssetsTest {

    @Test
    void dashboardAssetsAreResponsiveAndReadOnly() throws IOException {
        final String html = resource("/magazineAssets/magazineIndex.html");
        final String css = resource("/magazineAssets/style.css");

        assertFalse(html.contains("READ ONLY / PEEK ENABLED"));
        assertTrue(html.contains("id=\"peek-shard\""));
        assertTrue(html.contains("id=\"peek-pointer\""));
        assertTrue(html.contains("/magazine/v1/magazines"));
        assertTrue(html.contains("href=\"style.css\""));
        assertTrue(html.contains("load counter"));
        assertTrue(html.contains("fire counter"));
        assertTrue(html.contains("setInterval"));
        assertTrue(html.contains("30000"));
        assertTrue(css.contains("@media (max-width:760px)"));
        assertFalse(html.contains("/load"));
        assertFalse(html.contains("/reload"));
        assertFalse(html.contains("/fire"));
        assertFalse(html.contains("method:'DELETE'"));
    }

    /**
     * The CSS column counts are not derivable from the markup, so they drift silently. Both of
     * these broke unnoticed when the API stopped aggregating pointers (5 metrics -> 3) and when
     * peeked records gained a type field (3 columns -> 4).
     */
    @Test
    void gridColumnCountsMatchTheFieldsRendered() throws IOException {
        final String html = resource("/magazineAssets/magazineIndex.html");
        final String css = resource("/magazineAssets/style.css");

        assertTrue(html.contains("[['load counter','loadCounter'],['fire counter','fireCounter'],"
                        + "['pending','pending']]"),
                "totals render exactly three metrics - pointers do not aggregate across shards");
        assertTrue(css.contains(".metrics { display:grid; grid-template-columns:repeat(3,1fr);"),
                "the metrics grid must declare one column per rendered metric");

        for (String field : new String[]{"Shard", "Pointer", "Type", "Payload"}) {
            assertTrue(html.contains("<span>" + field + "</span>"), "peek renders " + field);
        }
        assertTrue(css.contains(".peek-result article { display:grid; "
                        + "grid-template-columns:100px 130px 130px 1fr;"),
                "the peek grid must declare one column per rendered field");
    }

    @Test
    void wireFormatIsPlainNumbers() throws IOException {
        final String html = resource("/magazineAssets/magazineIndex.html");

        // Counters and pointers are JSON numbers, not ToStringSerializer strings, so no BigInt
        // parsing is needed. What is needed is a guard against values JS cannot hold exactly.
        assertFalse(html.contains("BigInt"), "no BigInt handling once the wire format is numeric");
        assertTrue(html.contains("Number.isSafeInteger"), "reject pointers beyond 2^53");
        assertTrue(html.contains("aria-live"), "async panels announce updates");
    }

    private static String resource(final String path) throws IOException {
        try (InputStream stream = MagazineDashboardAssetsTest.class.getResourceAsStream(path)) {
            assertNotNull(stream, path);
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
