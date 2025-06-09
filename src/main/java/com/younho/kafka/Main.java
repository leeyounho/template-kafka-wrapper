package com.younho.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(MetricsConfig.class, KafkaConfig.class);
        String[] beanNames = context.getBeanDefinitionNames();
        for (String beanName : beanNames) {
            System.out.println(beanName);
        }

        while(true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
//        KafkaConfig kafkaConfig = context.getBean(KafkaConfig.class);
//        kafkaConfig.setBootstrapServers("localhost:9092");
//        kafkaConfig.setConsumerGroupId("my-group");
//        kafkaConfig.setReplyConsumerGroupId("my-group-reply");
//        kafkaConfig.setMySubject("TEST-TOPIC");
//        kafkaConfig.setDestSubject("TEST-TOPIC-2");
//        KafkaWrapper kafkaWrapper = kafkaConfig.createInstance();
//        kafkaWrapper.init();
//
//        ReplyKafkaWrapper replyKafkaWrapper = new ReplyKafkaWrapper("localhost:9092", "TEST-TOPIC-2", "TEST-TOPIC");
//        replyKafkaWrapper.init();
    }
}