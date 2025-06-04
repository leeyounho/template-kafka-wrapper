package com.younho.kafka;

import com.younho.ReplyKafkaWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Map;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        ApplicationContext context = new AnnotationConfigApplicationContext(MetricsConfig.class, KafkaConfig.class);

        KafkaConfig kafkaConfig = context.getBean(KafkaConfig.class);
        kafkaConfig.setBootstrapServers("localhost:9092");
        kafkaConfig.setConsumerGroupId("my-group");
        kafkaConfig.setReplyConsumerGroupId("my-group-reply");
        kafkaConfig.setMySubject("TEST-TOPIC");
        kafkaConfig.setDestSubject("TEST-TOPIC-2");
        KafkaWrapper kafkaWrapper = kafkaConfig.createInstance();
        kafkaWrapper.init();

        ReplyKafkaWrapper replyKafkaWrapper = new ReplyKafkaWrapper("localhost:9092", "TEST-TOPIC-2", "TEST-TOPIC");
        replyKafkaWrapper.init();

        Thread.sleep(5000);

        KafkaMsg kafkaMsg = new KafkaMsg();
//        kafkaMsg.setKey("TEST_KEY");

        Map<String, Object> map = kafkaMsg.getValue();
        map.put("TEST_VALUE", "TEST_VALUE");

        kafkaMsg.setValue(map);

//        try {
//            kafkaWrapper.send(kafkaMsg);
//        } catch (Exception e) {
//        }
//        Thread.sleep(3000);

        try {
            kafkaWrapper.sendRequest(kafkaMsg);
        } catch (Exception e) {
        }
    }
}