package com.younho.kafka;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class PrometheusExporter {
    private static final Logger logger = LoggerFactory.getLogger(PrometheusExporter.class);
    private static final int PORT = 9091; // Prometheus가 스크래핑할 포트
    private static final String PATH = "/prometheus"; // 엔드포인트 경로

    private HttpServer server;

    private final PrometheusMeterRegistry prometheusRegistry;

    public PrometheusExporter(PrometheusMeterRegistry prometheusRegistry) {
        this.prometheusRegistry = prometheusRegistry;
    }

    @PostConstruct
    public void start() {
        try {
            // HTTP 서버 생성
            server = HttpServer.create(new InetSocketAddress(PORT), 0);

            // HTTP 엔드포인트 생성
            server.createContext(PATH, httpExchange -> {
                // 메트릭 스크랩
                String response = prometheusRegistry.scrape();
                byte[] responseBytes = response.getBytes("UTF-8");

                // HTTP 응답 헤더 설정
                httpExchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                httpExchange.sendResponseHeaders(200, responseBytes.length);

                // HTTP 응답 본문 전송
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(responseBytes);
                }
            });

            // 4. 별도 스레드에서 서버 시작
            new Thread(server::start).start();

            logger.info("Prometheus Exporter started on port {}", PORT);

        } catch (IOException e) {
            throw new RuntimeException("Failed to start Prometheus Exporter", e);
        }
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            server.stop(1); // 1초 대기 후 종료
            logger.info("Prometheus Exporter stopped.");
        }
    }
}
