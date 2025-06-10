package com.younho.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.logging.LogbackMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {
    private static final Logger logger = LoggerFactory.getLogger(MetricsConfig.class);

    @Bean
    public PrometheusMeterRegistry prometheusMeterRegistry() {
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }

    @Bean
    public PrometheusExporter prometheusExporter(PrometheusMeterRegistry prometheusMeterRegistry) {
        return new PrometheusExporter(prometheusMeterRegistry);
    }

    @Bean
    public MeterRegistry meterRegistry(PrometheusMeterRegistry prometheusMeterRegistry) {
        CompositeMeterRegistry registry = new CompositeMeterRegistry();

        // jvm
        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmHeapPressureMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);

        // system
        new ProcessorMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);

        // logging
        new LogbackMetrics().bindTo(registry);

        // database
        // TODO

        // threadpool
        // TODO

        // kafka producer
        // TODO

        // kafka consumer
        // TODO

        registry.add(prometheusMeterRegistry);
        return registry;
    }
}
