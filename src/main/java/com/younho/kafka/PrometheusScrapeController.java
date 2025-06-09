package com.younho.kafka;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PrometheusScrapeController {
    private final PrometheusMeterRegistry prometheusRegistry;

    // MetricsConfig에서 Bean으로 등록한 PrometheusMeterRegistry를 주입받습니다.
    public PrometheusScrapeController(PrometheusMeterRegistry prometheusRegistry) {
        this.prometheusRegistry = prometheusRegistry;
    }

    @GetMapping(value = "/prometheus", produces = "text/plain; version=0.0.4")
    public String prometheus() {
        // Prometheus가 수집할 수 있는 텍스트 포맷으로 메트릭을 반환합니다.
        return prometheusRegistry.scrape();
    }
}
