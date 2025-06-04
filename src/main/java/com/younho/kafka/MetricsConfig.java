package com.younho.kafka;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.core.instrument.binder.logging.LogbackMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class MetricsConfig {
    @Bean
    public MeterRegistry kafkaProducerMeterRegistry() {
        MeterRegistry registry = getMeterRegistry(Duration.ofSeconds(10), "metrics.kafka.producer");
        return registry;
    }

    @Bean
    public MeterRegistry kafkaConsumerMeterRegistry() {
        MeterRegistry registry = getMeterRegistry(Duration.ofSeconds(10), "metrics.kafka.consumer");
        return registry;
    }

    // TODO database monitoring
    @Bean
    public MeterRegistry databaseMeterRegistry() {
        MeterRegistry registry = getMeterRegistry(Duration.ofSeconds(10), "metrics.database");
        return registry;
    }

    // TODO threadpool monitoring
    @Bean
    public MeterRegistry executorServiceMeterRegistry() {
        MeterRegistry registry = getMeterRegistry(Duration.ofSeconds(10), "metrics.executor.service");
        return registry;
    }


    @Bean
    public MeterRegistry jvmMeterRegistry() {
        MeterRegistry registry = getMeterRegistry(Duration.ofSeconds(10), "metrics.jvm");

        new ClassLoaderMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmHeapPressureMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new JvmThreadDeadlockMetrics().bindTo(registry);

        return registry;
    }

    @Bean
    public MeterRegistry systemMeterRegistry() {
        MeterRegistry registry = getMeterRegistry(Duration.ofSeconds(10), "metrics.system");

        new ProcessorMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);

        return registry;
    }

    @Bean
    public MeterRegistry loggingMeterRegistry() {
        MeterRegistry registry = getMeterRegistry(Duration.ofSeconds(10), "metrics.logging");

        new LogbackMetrics().bindTo(registry);

        return registry;
    }

    private MeterRegistry getMeterRegistry(Duration step, String loggerName) {
        Logger logger = LoggerFactory.getLogger(loggerName);

        LoggingRegistryConfig config = new LoggingRegistryConfig() {
            @Override
            public Duration step() {
                return step;
            }

            @Override
            public String get(String key) {
                return null;
            }
        };
        LoggingMeterRegistry registry = new LoggingMeterRegistry(config, Clock.SYSTEM, logger::info);
        registry.config().commonTags("server.name", "SERVER_NAME", "app.name", "APP_NAME")
                .meterFilter(MeterFilter.ignoreTags("spring.id"));
        return registry;
    }
}
