package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CrptApi {
    public static void main(String[] args) throws InterruptedException {
        LimitedRateExecutorService executorService = new LimitedRateExecutorService(40, SECONDS);
        HttpClient httpClient = HttpClient.newHttpClient();
        ObjectMapper objectMapper = new ObjectMapper();
        String documentCreateUrl = "https://ismp.crpt.ru/api/v3/lk/documents/create";
//        Creator documentCreator = new DocumentCreator(executorService, httpClient, objectMapper, documentCreateUrl);
        Creator documentCreator = new FakeDocumentCreator(executorService);
        var document = new DocumentCreationRequest();
        var sign = "sign";
        Runnable task = () -> documentCreator.create(document, sign);
        createThread(task, 1000);
        SECONDS.sleep(30);
        createThread(task, 1000);
        SECONDS.sleep(30);
        createThread(task, 100);
    }

    private static <T> void createThread(Runnable task, int threadCount) throws InterruptedException {
        while (threadCount > 0) {
            threadCount--;
            new Thread(task).start();
            TimeUnit.MILLISECONDS.sleep(5);
        }
    }

    public interface Creator {
        DocumentCreationResponse create(DocumentCreationRequest request, String sign);
    }

    public static class FakeDocumentCreator implements Creator {
        private final LimitedRateExecutorService executor;

        public FakeDocumentCreator(LimitedRateExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public DocumentCreationResponse create(DocumentCreationRequest request, String sign) {
            try {
                var task = new FutureTask<>(() -> {
                    SECONDS.sleep(2);
                    return new DocumentCreationResponse();
                });
                return executor.execute(task);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class DocumentCreator implements Creator {
        private final LimitedRateExecutorService executor;
        private final String DOCUMENT_CREATE_URL;
        private final HttpClient client;
        private final ObjectMapper objectMapper;

        public DocumentCreator(LimitedRateExecutorService executor, HttpClient httpClient, ObjectMapper objectMapper, String documentCreateUrl) {
            assert executor!= null;
            assert httpClient!= null;
            assert objectMapper!= null;
            assert documentCreateUrl!= null;

            this.client = HttpClient.newHttpClient();
            this.DOCUMENT_CREATE_URL = documentCreateUrl;
            this.executor = executor;
            this.objectMapper = new ObjectMapper();
        }

        public DocumentCreationResponse create(DocumentCreationRequest documentCreationRequest, String sign) {
            assert documentCreationRequest!= null;
            assert sign!= null;
            try {
                var requestUri = new URI(DOCUMENT_CREATE_URL + "?sign=" + sign);
                var body = objectMapper.writeValueAsString(documentCreationRequest);
                var request = HttpRequest.newBuilder(requestUri)
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
                var task = new FutureTask<DocumentCreationResponse>(() -> {
                    var response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    return objectMapper.readValue(response.body(), DocumentCreationResponse.class);
                });
                return executor.execute(task);
            } catch (URISyntaxException | InterruptedException | ExecutionException | JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }


    public static class LimitedRateExecutorService {
        private final TimeUnit timeUnit;
        private final int requestLimit;
        private final Semaphore semaphore;
        private ScheduledExecutorService scheduler;
        private final AtomicInteger dept;

        public LimitedRateExecutorService(int requestLimit, TimeUnit timeUnit) {
            assert requestLimit > 0;
            assert timeUnit!= null;
            this.timeUnit = timeUnit;
            this.requestLimit = requestLimit;
            this.semaphore = new Semaphore(requestLimit, true);
            this.dept = new AtomicInteger(0);
        }

        /**
         * Каждую единицу времени нужно освобождать занятые permits,
         * но тк некоторые задачи могут быть заблокированы в этот момент, мы выпускаем еще новых permits "в долг".
         * Как только разблокированные задачи вернут занятые permits "долг" будет погашен
         */
        private synchronized void resetSemaphore() {
            var availablePermits = semaphore.availablePermits();
            var permitsToRelease = requestLimit - availablePermits;
            if (permitsToRelease == 0) {
                stopScheduler();
            }
            dept.addAndGet(permitsToRelease);
            System.out.printf("\nResetting semaphore. Releasing %s permits\n\n", permitsToRelease);
            semaphore.release(permitsToRelease);
        }

        public <T> T execute(FutureTask<T> task) throws InterruptedException, ExecutionException {
            checkScheduler();
            semaphore.acquire();
            try {
                System.out.printf("<< Permits has been acquired: %s -> %s\n", semaphore.availablePermits() + 1, semaphore.availablePermits());
                return task.get();
            } finally {
                releasePermit();
            }
        }

        /**
         * Проверить работает ли scheduler
         */
        private synchronized void checkScheduler() {
            if (this.scheduler == null || this.scheduler.isShutdown()) {
                startScheduler();
            }
        }

        /**
         * Запустить новый scheduler
         */
        private void startScheduler() {
            System.out.println("Starting scheduler");
            this.scheduler = Executors.newSingleThreadScheduledExecutor();
            this.scheduler.scheduleAtFixedRate(this::resetSemaphore, 1, 1, timeUnit);
        }

        /**
         * Остановить scheduler
         */
        private void stopScheduler() {
            System.out.println("Shutting down scheduler");
            this.scheduler.shutdown();
            this.scheduler = null;
        }


        /**
         * Проверить есть ли долг и если есть в начале вернуть его
         */
        private synchronized void releasePermit() {
            if (dept.get() > 0) {
                dept.decrementAndGet();
            } else {
                var availablePermits = semaphore.availablePermits();
                System.out.printf(">> Permits has been released: %s -> %s\n", availablePermits, availablePermits + 1);
                semaphore.release();
            }
        }
    }

    public record DocumentCreationResponse() {
    }

    public static class DocumentCreationRequest {
        private Description description;
        private String docId;
        private String docStatus;
        private String docType;
        private boolean importRequest;
        private String ownerInn;
        private String participantInn;
        private String producerInn;
        private LocalDate productionDate;
        private String productionType;
        private List<Product> products;
        private LocalDate regDate;
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

        public String getDocType() {
            return docType;
        }

        public boolean isImportRequest() {
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

        public LocalDate getProductionDate() {
            return productionDate;
        }

        public String getProductionType() {
            return productionType;
        }

        public List<Product> getProducts() {
            return products;
        }

        public LocalDate getRegDate() {
            return regDate;
        }

        public String getRegNumber() {
            return regNumber;
        }


        public static class Description {
            private String participantInn;

            public String getParticipantInn() {
                return participantInn;
            }

            public void setParticipantInn(String participantInn) {
                this.participantInn = participantInn;
            }
        }

        public static class Product {
            private String certificateDocument;
            private LocalDate certificateDocumentDate;
            private String certificateDocumentNumber;
            private String ownerInn;
            private String producerInn;
            private LocalDate productionDate;
            private String tnvedCode;
            private String uitCode;
            private String uituCode;
        }

        public void setDescription(Description description) {
            this.description = description;
        }

        public void setDocId(String docId) {
            this.docId = docId;
        }

        public void setDocStatus(String docStatus) {
            this.docStatus = docStatus;
        }

        public void setDocType(String docType) {
            this.docType = docType;
        }

        public void setImportRequest(boolean importRequest) {
            this.importRequest = importRequest;
        }

        public void setOwnerInn(String ownerInn) {
            this.ownerInn = ownerInn;
        }

        public void setParticipantInn(String participantInn) {
            this.participantInn = participantInn;
        }

        public void setProducerInn(String producerInn) {
            this.producerInn = producerInn;
        }

        public void setProductionDate(LocalDate productionDate) {
            this.productionDate = productionDate;
        }

        public void setProductionType(String productionType) {
            this.productionType = productionType;
        }

        public void setProducts(List<Product> products) {
            this.products = products;
        }

        public void setRegDate(LocalDate regDate) {
            this.regDate = regDate;
        }

        public void setRegNumber(String regNumber) {
            this.regNumber = regNumber;
        }
    }
}