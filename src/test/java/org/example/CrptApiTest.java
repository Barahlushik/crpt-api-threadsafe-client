package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;


public class CrptApiTest {

    private WireMockServer wm;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void start() {
        wm = new WireMockServer(0);
        wm.start();
        configureFor("localhost", wm.port());
    }

    @AfterEach
    void stop() {
        if (wm != null) wm.stop();
    }

    private CrptApi newApi() {
        return new CrptApi.Builder(TimeUnit.SECONDS, 10)
                .baseUri(URI.create("http://localhost:" + wm.port()))
                .createPath("/api/v3/lk/documents/create")
                .pgQuery("milk")
                .productGroup("milk")
                .requestTimeout(Duration.ofSeconds(5))
                .build();
    }

    private CrptApi.Document sampleDoc() {
        return new CrptApi.Document()
                .setDocId("DOC-42")
                .setDocStatus("DRAFT")
                .setDocType(CrptApi.DocType.LP_INTRODUCE_GOODS)
                .setOwnerInn("1234567890")
                .setParticipantInn("1234567890")
                .setProducerInn("1234567890")
                .setProductionDate("2025-08-22")
                .setProductionType("OWN_PRODUCTION")
                .setProducts(List.of(
                        new CrptApi.Product()
                                .setOwnerInn("1234567890")
                                .setProducerInn("1234567890")
                                .setProductionDate("2025-08-22")
                                .setTnvedCode("6403")
                                .setUitCode("0000000000000000000000000000000001")
                                    ))
                .setRegDate("2025-08-22");
    }

    @Test
    void shouldCallCorrectEndpointAndBodyIsValid() throws Exception {
        wm.stubFor(post(urlPathEqualTo("/api/v3/lk/documents/create"))
                .withQueryParam("pg", equalTo("milk"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"ok\":true}")));

        CrptApi api = newApi();

        var res = api.createDocument(sampleDoc(), "BASE64_SIGNATURE");
        assertEquals(200, res.getStatusCode());
        assertTrue(res.getRawBody().orElse("").contains("\"ok\":true"));

        wm.verify(postRequestedFor(urlPathEqualTo("/api/v3/lk/documents/create"))
                .withQueryParam("pg", equalTo("milk"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withHeader("Accept", equalTo("application/json")));

        List<ServeEvent> events = wm.getAllServeEvents();
        assertFalse(events.isEmpty(), "должен быть как минимум один вызов");
        String body = events.get(0).getRequest().getBodyAsString();

        JsonNode root = mapper.readTree(body);
        assertEquals("MANUAL", root.get("document_format").asText());
        assertEquals("LP_INTRODUCE_GOODS", root.get("type").asText());
        assertEquals("milk", root.get("product_group").asText());
        assertEquals("BASE64_SIGNATURE", root.get("signature").asText());

        String b64 = root.get("product_document").asText();
        String decodedJson = new String(Base64.getDecoder().decode(b64), StandardCharsets.UTF_8);
        JsonNode docNode = mapper.readTree(decodedJson);

        assertEquals("DOC-42", docNode.get("doc_id").asText());
        assertEquals("DRAFT", docNode.get("doc_status").asText());
        assertEquals("LP_INTRODUCE_GOODS", docNode.get("doc_type").asText());
        assertEquals("1234567890", docNode.get("owner_inn").asText());
        assertEquals("OWN_PRODUCTION", docNode.get("production_type").asText());
        assertTrue(docNode.get("products").isArray());
        assertEquals(1, docNode.get("products").size());
        assertEquals("6403", docNode.get("products").get(0).get("tnved_code").asText());
    }

    @Test
    void shouldBlockOnRateLimit_2PerSecond_thirdWaits() throws Exception {
        wm.stubFor(post(urlPathEqualTo("/api/v3/lk/documents/create"))
                .withQueryParam("pg", equalTo("milk"))
                .willReturn(aResponse().withStatus(200)));

        CrptApi api = new CrptApi.Builder(TimeUnit.SECONDS, 2)
                .baseUri(URI.create("http://localhost:" + wm.port()))
                .createPath("/api/v3/lk/documents/create")
                .pgQuery("milk")
                .requestTimeout(Duration.ofSeconds(5))
                .build();

        long now = System.nanoTime();

        api.createDocument(sampleDoc(), "S1");
        api.createDocument(sampleDoc(), "S2");
        // !!!!--------------------------BLOCK----------------------------!!!!
        api.createDocument(sampleDoc(), "S3");

        long elapsedMs = (System.nanoTime() - now) / 1_000_000L;
        System.out.println("Elapsed: " + elapsedMs + " ms");

        assertTrue(elapsedMs >= 1000,
                "ожидали блокировку ~1s при лимите 2/сек, фактически " + elapsedMs + " ms");
    }

    @Test
    void shouldBlockOnRateLimit_5PerSecond_thirdWaits() throws Exception {
        wm.stubFor(post(urlPathEqualTo("/api/v3/lk/documents/create"))
                .withQueryParam("pg", equalTo("milk"))
                .willReturn(aResponse().withStatus(200)));

        CrptApi api = new CrptApi.Builder(TimeUnit.SECONDS, 5)
                .baseUri(URI.create("http://localhost:" + wm.port()))
                .createPath("/api/v3/lk/documents/create")
                .pgQuery("milk")
                .requestTimeout(Duration.ofSeconds(5))
                .build();

        long now = System.nanoTime();

        api.createDocument(sampleDoc(), "S1"); // проходит сразу
        api.createDocument(sampleDoc(), "S2"); // проходит сразу
        api.createDocument(sampleDoc(), "S3"); // проходит сразу
        api.createDocument(sampleDoc(), "S4"); // проходит сразу
        api.createDocument(sampleDoc(), "S5"); // проходит сразу
        // -------------------------------BLOCK--------------------------------
        api.createDocument(sampleDoc(), "S6"); // должен ждать ~1с

        long elapsedMs = (System.nanoTime() - now) / 1_000_000L;
        System.out.println("Elapsed: " + elapsedMs + " ms");

        assertTrue(elapsedMs >= 1000,
                "ожидали блокировку ~1s при лимите 5/сек, фактически " + elapsedMs + " ms");
    }
}
