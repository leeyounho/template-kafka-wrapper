# Kafka Wrapper Template

## 개요

이 프로젝트는 Spring Kafka를 기반으로 Kafka Producer 및 Consumer를 더 쉽게 설정하고 사용할 수 있도록 래핑한 Java 라이브러리입니다. 주요 기능은 다음과 같습니다:

* **간편한 설정**: Kafka 관련 설정을 `KafkaConfig` 클래스로 중앙에서 관리하여 손쉽게 인스턴스를 생성할 수 있습니다.
* **Request-Reply 패턴 지원**: `ReplyingKafkaTemplate`을 사용하여 동기식 메시지 요청 및 응답 패턴을 지원합니다.
* **커스텀 (역)직렬화**: `KafkaMsg`라는 자체 메시지 형식을 정의하고, 이에 대한 JSON 기반의 (역)직렬화기(`KafkaMsgSerializer`, `KafkaMsgDeserializer`)를 제공합니다. 이를 통해 메시지 헤더와 바디를 함께 관리할 수 있습니다.
* **메트릭 수집**: Micrometer를 사용하여 Kafka Producer/Consumer 및 JVM 관련 메트릭을 수집하고 로깅하는 기능을 포함합니다.
* **테스트 용이성**: `EmbeddedKafka`를 사용한 통합 테스트 예제를 포함하여, 개발 및 테스트가 용이하도록 구성되어 있습니다.

## 주요 구성 요소

### Core Classes

* `kafka.KafkaWrapper`: Kafka Producer와 Consumer를 래핑하여 메시지 송수신 및 Request-Reply 기능을 제공하는 메인 클래스입니다.
* `kafka.KafkaConfig`: Kafka 연결 정보(Bootstrap a aServers, Topic, Group ID 등) 및 Producer/Consumer 속성을 설정하는 클래스입니다.
* `ReplyKafkaWrapper`: 요청(Request) 메시지를 받아 응답(Reply) 메시지를 보내는 역할의 예제 클래스입니다.
* `kafka.KafkaMsg`: Kafka 메시지의 본문(`value`)과 헤더(`headers`)를 포함하는 데이터 객체입니다. JSON으로 직렬화되어 전송됩니다.
* `kafka.KafkaMsgSerializer` / `kafka.KafkaMsgDeserializer`: `KafkaMsg` 객체를 직렬화/역직렬화하는 클래스입니다. 메시지 헤더 정보를 Kafka 네이티브 헤더로 자동 변환합니다.

### Test Classes

* `KafkaTemplateTest`: `KafkaWrapper`를 사용하여 메시지를 보내는 기본 기능을 테스트합니다.
* `ReplyingKafkaTemplateTest`: Request-Reply 패턴이 정상적으로 동작하는지 테스트합니다.
* `SerializationTest`: `KafkaMsg` 객체가 다양한 데이터 타입(byte[], String, Integer)을 포함할 때 직렬화 및 역직렬화가 올바르게 수행되는지 검증합니다.
* `OtherSerializationTest`: 다른 형식의 메시지 객체(`OtherKafkaMsg`)와의 직렬화 호환성을 테스트합니다.

## 시작하기

### 요구사항

* Java 8 이상
* Gradle 8.10

### 설정 및 실행

1.  **Kafka 서버 정보 설정**:
    `com.younho.kafka.Main` 클래스에서 Kafka 서버의 주소와 토픽 이름 등을 설정합니다.

    ```java
    // src/main/java/com/younho/kafka/Main.java

    // ...
    KafkaConfig kafkaConfig = context.getBean(KafkaConfig.class);
    kafkaConfig.setBootstrapServers("localhost:9092"); // Kafka 서버 주소
    kafkaConfig.setConsumerGroupId("my-group");
    kafkaConfig.setMySubject("TEST-TOPIC");           // 수신할 토픽
    kafkaConfig.setDestSubject("TEST-TOPIC-2");       // 발신할 토픽
    // ...
    ```

2.  **애플리케이션 실행**:
    `Main.main()` 메소드를 실행하면 `KafkaWrapper`와 `ReplyKafkaWrapper`가 초기화됩니다. `KafkaWrapper`는 `TEST-TOPIC-2`로 Request 메시지를 보내고, `ReplyKafkaWrapper`는 해당 메시지를 수신하여 다시 `TEST-TOPIC`으로 응답 메시지를 보냅니다.

### 테스트 실행

Gradle을 사용하여 전체 테스트를 실행할 수 있습니다.

```bash
./gradlew test