package org.example;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>единый метод создания/внесения документа
 * "Ввод в оборот товара, произведённого в РФ": {@code /api/v3/lk/documents/create}.
 * <p>Use case:
 * <pre>{@code
 * CrptApi api = new CrptApi.Builder(TimeUnit.SECONDS, 5)
 *     .baseUri(URI.create("https://ismp.crpt.ru"))
 *     .createPath("/api/v3/lk/documents/create")
 *     .pgQuery("milkshake")
 *     .productGroup("milk")
 *     .bearer("TOKEN")
 *     .requestTimeout(Duration.ofSeconds(30))
 *     .build();
 *
 * // внутренний документ :
 * CrptApi.Document doc = new CrptApi.Document()
 *     .setDocId("123")
 *     .setOwnerInn("1234567890");
 * // нужные поля (DTO ниже)
 *
 * String detachedSignatureBase64 = "BASE64_GOST_SIGNATURE";
 * CrptApi.CreateDocumentRequest response = api.createDocument(doc, detachedSignatureBase64);
 * }</pre>
 */

public class CrptApi {
    /**
     * "https://ismp.crpt.ru"
     */
    private final URI baseUri;
    /**
     * "/api/v3/lk/documents/create"
     */
    private final String createPath;
    /**
     * По умолчанию из java.net
     */
    private final HttpClient http;
    /**
     * По умолчанию реализация Jackson
     */
    private final ObjectMapper mapper;
    /**
     * Таймаут запроса HTTP уровня клиента.
     */
    private final Duration requestTimeout;
    /**
     * Security.
     */
    private final AuthProvider authProvider;
    /**
     * Реализация блокировки
     **/
    private final RateLimiter rateLimiter;
    /**
     * Builder для модульности
     */
    private final Builder builder;

    /**
     * Конструктор по умолчанию.
     */
    public CrptApi(TimeUnit units, int rate) {
        this(new Builder(units, rate));
    }

    /**
     * Внутренний конструктор от билдера. Инжектит дефолтные настройки, если не указаны иные.
     */
    private CrptApi(Builder b) {
        this.builder = b;
        this.baseUri = b.baseUri;
        this.createPath = b.createPath;
        this.http = b.httpClient != null ? b.httpClient : HttpClient.newBuilder().connectTimeout(b.requestTimeout).build();
        this.mapper = b.objectMapper != null ? b.objectMapper : defaultMapper();
        this.requestTimeout = b.requestTimeout;
        this.authProvider = b.authProvider;
        this.rateLimiter = new RateLimiter(b.nanos, b.requestLimit);
    }

    /**
     * Создание документа "Ввод в оборот (РФ)"».
     * <p>
     * Метод блокирующий: при превышении лимита запросов — ожидает (см. {@link RateLimiter#block()}).
     *
     * @param productDocument Java-объект внутреннего документа.
     * @param signatureBase64 base64(подпись документа) .
     * @return {@link CreateDocumentResponse} со статусом и телом ответа сервера (при наличии).
     * @throws IOException          сетевые/IO ошибки во время HTTP-вызова
     * @throws InterruptedException если текущий поток прервали во время ожидания лимита или HTTP-вызова
     */
    public CreateDocumentResponse createDocument(Object productDocument, String signatureBase64)
            throws IOException, InterruptedException {
        try {
            Objects.requireNonNull(productDocument, "productDocument must not be null");
            Objects.requireNonNull(signatureBase64, "signature must not be null");

            // TIME TO BLOCK
            rateLimiter.block();

            String productJson = mapper.writeValueAsString(productDocument);
            String productBase64 = Base64.getEncoder().encodeToString(productJson.getBytes(StandardCharsets.UTF_8));

            CreateDocumentRequest body = new CreateDocumentRequest(
                    "MANUAL",
                    productBase64,
                    builder.productGroup,
                    signatureBase64,
                    "LP_INTRODUCE_GOODS"
            );

            String payload = mapper.writeValueAsString(body);
            URI uri = baseUri.resolve(createPathWithPg(createPath, builder.pgQuery));
            HttpRequest.Builder rq = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(requestTimeout)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json");

            if (authProvider != null) {
                String[] headers = authProvider.authHeaders();
                for (int i = 0; i + 1 < headers.length; i += 2) {
                    rq.header(headers[i], headers[i + 1]);
                }
            }

            HttpRequest request = rq.POST(HttpRequest.BodyPublishers.ofString(payload)).build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());

            CreateDocumentResponse result = new CreateDocumentResponse(
                    response.statusCode(),
                    Optional.ofNullable(response.body()).filter(s -> !s.isEmpty()));

            result.getRawBody().ifPresent(b -> {
                try {
                    mapper.readerForUpdating(result).readValue(b);
                } catch (JsonProcessingException e) {
                    throw new CrptParseException("Fail to parse CreateDocumentResponse", e);
                }
            });

            return result;

        } catch (Exception e) {
            return new CreateDocumentResponse(
                    404,
                    Optional.ofNullable(e.getMessage())
            );
        }
    }


    /**
     * Ошибка парсинга CreateDocumentResponse .
     * Возникает, если Jackson не смог десериализовать тело ответа в DTO.
     */
    public static class CrptParseException extends RuntimeException {

        public CrptParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Реализация Builder.
     * <p>
     * Обязательные параметры:
     * <ul>
     *   <li>{@code timeUnit} — интервал времени</li>
     *   <li>{@code requestLimit} — максимум запросов за интервал времени</li>
     * </ul>
     */
    public static final class Builder {
        /**
         * Длительность интервала времени в наносекундах (1 * timeUnit).
         */
        private final long nanos;
        private final int requestLimit;
        private URI baseUri = URI.create("https://ismp.crpt.ru");
        private String createPath = "/api/v3/lk/documents/create";
        private String pgQuery = null;
        private String productGroup = "milk";
        private Duration requestTimeout = Duration.ofSeconds(30);
        private AuthProvider authProvider = AuthProvider.noAuth();
        private HttpClient httpClient;
        private ObjectMapper objectMapper;

        public Builder(TimeUnit timeUnit, int requestLimit) {
            Objects.requireNonNull(timeUnit, "timeUnit");
            if (requestLimit <= 0) throw new IllegalArgumentException("requestLimit must be > 0");
            this.nanos = timeUnit.toNanos(1);
            this.requestLimit = requestLimit;
        }

        public Builder baseUri(URI baseUri) {
            this.baseUri = Objects.requireNonNull(baseUri);
            return this;
        }

        public Builder createPath(String path) {
            this.createPath = Objects.requireNonNull(path);
            return this;
        }

        public Builder pgQuery(String pg) {
            this.pgQuery = pg;
            return this;
        }

        public Builder productGroup(String productGroup) {
            this.productGroup = productGroup;
            return this;
        }

        public Builder bearer(String token) {
            this.authProvider = AuthProvider.bearer(token);
            return this;
        }

        public Builder authProvider(AuthProvider provider) {
            this.authProvider = Objects.requireNonNull(provider);
            return this;
        }

        public Builder requestTimeout(Duration timeout) {
            this.requestTimeout = Objects.requireNonNull(timeout);
            return this;
        }

        public Builder httpClient(HttpClient httpClient) {
            this.httpClient = Objects.requireNonNull(httpClient);
            return this;
        }

        public Builder objectMapper(ObjectMapper mapper) {
            this.objectMapper = Objects.requireNonNull(mapper);
            return this;
        }

        public CrptApi build() {
            return new CrptApi(this);
        }
    }

    private static String createPathWithPg(String path, String pg) {
        if (pg == null || pg.isEmpty()) return path;
        String q = "pg=" + URLEncoder.encode(pg, StandardCharsets.UTF_8);
        return path.contains("?") ? path + "&" + q : path + "?" + q;
    }


    /**
     * Пример схемы документа, взятый из документации.
     *
     * {
     *   "description": {
     *     "participantInn": "string"
     *   },
     *   "doc_id": "string",
     *   "doc_status": "string",
     *   "doc_type": "LP_INTRODUCE_GOODS",
     *   "importRequest": true,
     *   "owner_inn": "string",
     *   "participant_inn": "string",
     *   "producer_inn": "string",
     *   "production_date": "2020-01-23",
     *   "production_type": "string",
     *   "products": [
     *     {
     *       "certificate_document": "string",
     *       "certificate_document_date": "2020-01-23",
     *       "certificate_document_number": "string",
     *       "owner_inn": "string",
     *       "producer_inn": "string",
     *       "production_date": "2020-01-23",
     *       "tnved_code": "string",
     *       "uit_code": "string",
     *       "uitu_code": "string"
     *     }
     *   ],
     *   "reg_date": "2020-01-23",
     *   "reg_number": "string"
     * }
     */

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Document {

        @JsonProperty("description")
        private Description description;
        @JsonProperty("doc_id")
        private String docId;
        @JsonProperty("doc_status")
        private String docStatus;
        @JsonProperty("doc_type")
        private DocType docType;
        @JsonProperty("importRequest")
        private boolean importRequest;
        @JsonProperty("owner_inn")
        private String ownerInn;
        @JsonProperty("participant_inn")
        private String participantInn;
        @JsonProperty("producer_inn")
        private String producerInn;
        @JsonProperty("production_date")
        private String productionDate;
        @JsonProperty("production_type")
        private String productionType;
        @JsonProperty("products")
        private List<Product> products;
        @JsonProperty("reg_date")
        private String regDate;
        @JsonProperty("reg_number")
        private String regNumber;

        public Description getDescription() {
            return description;
        }

        public String getDocId() {
            return docId;
        }

        public String getDocStatus() {
            return docStatus;
        }

        public DocType getDocType() {
            return docType;
        }

        public boolean getImportRequest() {
            return importRequest;
        }

        public String getOwnerInn() {
            return ownerInn;
        }

        public String getParticipantInn() {
            return participantInn;
        }

        public String getProducerInn() {
            return producerInn;
        }

        public String getProductionDate() {
            return productionDate;
        }

        public String getProductionType() {
            return productionType;
        }

        public List<Product> getProducts() {
            return products;
        }

        public String getRegDate() {
            return regDate;
        }

        public String getRegNumber() {
            return regNumber;
        }

        public Document setDescription(Description v) {
            this.description = v;
            return this;
        }

        public Document setDocId(String v) {
            this.docId = v;
            return this;
        }

        public Document setDocStatus(String v) {
            this.docStatus = v;
            return this;
        }

        public Document setDocType(DocType v) {
            this.docType = v;
            return this;
        }

        public Document setImportRequest(boolean v) {
            this.importRequest = v;
            return this;
        }

        public Document setOwnerInn(String v) {
            this.ownerInn = v;
            return this;
        }

        public Document setParticipantInn(String v) {
            this.participantInn = v;
            return this;
        }

        public Document setProducerInn(String v) {
            this.producerInn = v;
            return this;
        }

        public Document setProductionDate(String v) {
            this.productionDate = v;
            return this;
        }

        public Document setProductionType(String v) {
            this.productionType = v;
            return this;
        }

        public Document setProducts(java.util.List<Product> v) {
            this.products = v;
            return this;
        }

        public Document setRegDate(String v) {
            this.regDate = v;
            return this;
        }

        public Document setRegNumber(String v) {
            this.regNumber = v;
            return this;
        }

        public Document participantInnInDescription(String inn) {
            this.description = new Description().setParticipantInn(inn);
            return this;
        }
    }

    /**
     * Description
     * Вложенный объект с дополнительной информацией о документе.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class Description {
        @JsonProperty("participantInn")
        private String participantInn;

        public String getParticipantInn() {
            return participantInn;
        }

        public Description setParticipantInn(String v) {
            this.participantInn = v;
            return this;
        }
    }

    /**
     * Product
     * Вложенная коллекция с информацией о продуктах.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class Product {

        @JsonProperty("certificate_document")
        private String certificateDocument;
        @JsonProperty("certificate_document_date")
        private String certificateDocumentDate;
        @JsonProperty("certificate_document_number")
        private String certificateDocumentNumber;
        @JsonProperty("owner_inn")
        private String ownerInn;
        @JsonProperty("producer_inn")
        private String producerInn;
        @JsonProperty("production_date")
        private String productionDate;
        @JsonProperty("tnved_code")
        private String tnvedCode;
        @JsonProperty("uit_code")
        private String uitCode;
        @JsonProperty("uitu_code")
        private String uituCode;

        public String getCertificateDocument() {
            return certificateDocument;
        }

        public String getCertificateDocumentDate() {
            return certificateDocumentDate;
        }

        public String getCertificateDocumentNumber() {
            return certificateDocumentNumber;
        }

        public String getOwnerInn() {
            return ownerInn;
        }

        public String getProducerInn() {
            return producerInn;
        }

        public String getProductionDate() {
            return productionDate;
        }

        public String getTnvedCode() {
            return tnvedCode;
        }

        public String getUitCode() {
            return uitCode;
        }

        public String getUituCode() {
            return uituCode;
        }

        public Product setCertificateDocument(String v) {
            this.certificateDocument = v;
            return this;
        }

        public Product setCertificateDocumentDate(String v) {
            this.certificateDocumentDate = v;
            return this;
        }

        public Product setCertificateDocumentNumber(String v) {
            this.certificateDocumentNumber = v;
            return this;
        }

        public Product setOwnerInn(String v) {
            this.ownerInn = v;
            return this;
        }

        public Product setProducerInn(String v) {
            this.producerInn = v;
            return this;
        }

        public Product setProductionDate(String v) {
            this.productionDate = v;
            return this;
        }

        public Product setTnvedCode(String v) {
            this.tnvedCode = v;
            return this;
        }

        public Product setUitCode(String v) {
            this.uitCode = v;
            return this;
        }

        public Product setUituCode(String v) {
            this.uituCode = v;
            return this;
        }
    }

    /**
     * DocType
     * Тип документа (doc_type)
     */
    public enum DocType {
        LP_INTRODUCE_GOODS("LP_INTRODUCE_GOODS"),
        LP_INTRODUCE_GOODS_CSV("LP_INTRODUCE_GOODS_CSV"),
        LP_INTRODUCE_GOODS_XML("LP_INTRODUCE_GOODS_XML"),
        ;

        private final String value;

        DocType(String value) {
            this.value = value;
        }

        @JsonValue
        public String getValue() {
            return value;
        }

        @JsonCreator
        public static DocType fromValue(String v) {
            for (DocType t : values()) {
                if (t.value.equals(v)) return t;
            }
            throw new IllegalArgumentException("Неизвестный doc_type: " + v);
        }
    }

    /**
     * DTO Request к /documents/create
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private static final class CreateDocumentRequest {

        @JsonProperty("document_format")
        final String documentFormat;
        @JsonProperty("product_document")
        final String productDocumentBase64;
        @JsonProperty("product_group")
        final String productGroup;
        @JsonProperty("signature")
        final String signatureBase64;
        @JsonProperty("type")
        final String type;

        CreateDocumentRequest(String documentFormat,
                              String productDocumentBase64,
                              String productGroup,
                              String signatureBase64,
                              String type) {
            this.documentFormat = documentFormat;
            this.productDocumentBase64 = productDocumentBase64;
            this.productGroup = productGroup;
            this.signatureBase64 = signatureBase64;
            this.type = type;
        }
    }

    /**
     * DTO Response от /documents/create
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class CreateDocumentResponse {

        private final int statusCode;
        private final Optional<String> rawBody;
        @JsonProperty("value")
        private String value;
        @JsonProperty("code")
        private String code;
        @JsonProperty("error_message")
        private String errorMessage;
        @JsonProperty("description")
        private String description;

        public CreateDocumentResponse(int statusCode, Optional<String> rawBody) {
            this.statusCode = statusCode;
            this.rawBody = rawBody;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public Optional<String> getRawBody() {
            return rawBody;
        }

        public String getValue() {
            return value;
        }

        public String getCode() {
            return code;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public String getDescription() {
            return description;
        }

        public boolean isSuccess() {
            return value != null && (code == null || code.isEmpty());
        }
    }


    /**
     * Реализация блокировки при превышении лимита запросов, чтобы не превысить максимальное количество запросов к API
     * <br>
     * Идея проста: за последние windowNanos можно пропустить не больше requestLimit запросов.
     * Если лимит выбит — садим поток в очередь и будим по очереди (FIFO), как только освободимся от таймаута.
     */
    private static final class RateLimiter {
        private final long nanos;
        private final int requestLimit;

        private final ReentrantLock lock = new ReentrantLock();
        private final Deque<Long> timestampsNanos = new ArrayDeque<>();
        private final ArrayDeque<WaitNode> queue = new ArrayDeque<WaitNode>();

        private static final class WaitNode {
            final Condition c;

            WaitNode(Condition c) {
                this.c = c;
            }
        }

        RateLimiter(long nanos, int requestLimit) {
            if (requestLimit <= 0) throw new IllegalArgumentException("requestLimit must be > 0");
            if (nanos <= 0) throw new IllegalArgumentException("windowNanos must be > 0");
            this.nanos = nanos;
            this.requestLimit = requestLimit;
        }

        /**
         * Блокирующе ожидает возможности совершить запрос.
         */
        void block() throws InterruptedException {
            final long now = System.nanoTime();
            lock.lock();
            try {
                invalidateOld(now);
                if (timestampsNanos.size() < requestLimit && queue.isEmpty()) {
                    timestampsNanos.addLast(System.nanoTime());
                    return;
                }
                WaitNode me = new WaitNode(lock.newCondition());
                queue.addLast(me);

                while (true) {
                    boolean iAmHead = (queue.peekFirst() == me);
                    if (iAmHead) {
                        invalidateOld(System.nanoTime());
                        if (timestampsNanos.size() < requestLimit) {
                            timestampsNanos.addLast(System.nanoTime());
                            queue.removeFirst();
                            signalHeadIfNeeded();
                            return;
                        }
                        Long oldest = timestampsNanos.peekFirst();
                        if (oldest == null) {
                            continue;
                        }
                        long waitNanos = (oldest + nanos) - System.nanoTime();
                        if (waitNanos <= 0) {
                            continue;
                        }
                        me.c.awaitNanos(waitNanosIsSafe(waitNanos));
                    } else
                        me.c.await();

                }
            } finally {
                lock.unlock();
            }

        }

        private void signalHeadIfNeeded() {
            WaitNode head = queue.peekFirst();
            if (head != null)
                head.c.signal();

        }

        private long waitNanosIsSafe(long waitNanos) {
            final long max = TimeUnit.SECONDS.toNanos(10);
            return Math.min(waitNanos, max);

        }


        private void invalidateOld(long now) {
            long threshold = now - nanos;
            int before = timestampsNanos.size();

            while (!timestampsNanos.isEmpty() && timestampsNanos.peekFirst() < threshold)
                timestampsNanos.removeFirst();


            if (timestampsNanos.size() < before && !queue.isEmpty())
                signalHeadIfNeeded();

        }
    }

    /**
     * Security Provider статичная фабрика, просто ради прикола  ...
     */

    public interface AuthProvider {
        String[] authHeaders();

        static AuthProvider noAuth() {
            return () -> new String[0];
        }

        static AuthProvider bearer(String token) {
            if (token == null)
                throw new IllegalArgumentException("token is null");

            return () -> new String[]{"Authorization", "Bearer " + token};
        }
    }

    /**
     * Дефолтный конфиг маппера.
     */
    private static ObjectMapper defaultMapper() {
        ObjectMapper m = new ObjectMapper();
        m.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return m;
    }
}
