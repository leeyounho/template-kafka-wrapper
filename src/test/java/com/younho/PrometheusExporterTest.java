package com.younho;

import com.younho.kafka.PrometheusExporter;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PrometheusExporterTest {

    private PrometheusExporter exporter;

    @Mock // 가짜 PrometheusMeterRegistry 객체 생성
    private PrometheusMeterRegistry mockRegistry;

    @BeforeEach
    void setUp() {
        // 테스트 시작 전, mock 객체를 주입하여 Exporter 인스턴스 생성
        exporter = new PrometheusExporter(mockRegistry);
    }

    @AfterEach
    void tearDown() {
        // 각 테스트 후 서버 종료
        exporter.stop();
    }

    @Test
    @DisplayName("서버가 시작되고, scrape 요청 시 mock 데이터를 정상적으로 반환한다")
    void startAndScrapeTest() throws Exception {
        // given: mockRegistry.scrape()가 호출되면 "fake_metric 1.0"을 반환하도록 설정
        String fakeMetrics = "fake_metric 1.0";
        when(mockRegistry.scrape()).thenReturn(fakeMetrics);

        // when: 서버 시작
        exporter.start();

        // then: 실제 HTTP 요청을 보내 응답을 확인
        URL url = new URL("http://localhost:9091/prometheus");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");

        // 응답 코드 확인
        assertThat(con.getResponseCode()).isEqualTo(200);
        // Content-Type 헤더 확인
        assertThat(con.getHeaderField("Content-Type")).contains("text/plain");

        // 응답 본문 확인
        String responseBody = new BufferedReader(new InputStreamReader(con.getInputStream()))
                .lines().collect(Collectors.joining("\n"));
        assertThat(responseBody).isEqualTo(fakeMetrics);
    }
}
