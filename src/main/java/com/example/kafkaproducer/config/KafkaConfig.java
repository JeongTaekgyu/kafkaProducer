package com.example.kafkaproducer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    // 이 패턴이 템플릿을 만들고 커넥션 정보가 들어간 configuration을 만든다.
    // 그리고 그 configuration 메서드(여기서는 ProducerFactory())를 호출해서
    // 객체(여기선DefaultKafkaProducerFactory)를 받아서 템플릿을 형성한다.
    @Bean
    public KafkaTemplate<String, Object> KafkaTemplate() {
        return new KafkaTemplate<String, Object>(ProducerFactory());
    }

    @Bean
    public ProducerFactory<String, Object> ProducerFactory() {
        Map<String, Object> myConfig = new HashMap<>();
//        myConfig.put("bootstrap.servers", "localhost:9092");

        // 임의 로 아무 ip나 넣었는데 이거는 public ip를 넣어야한다. 왜냐하면 aws 안에서 돌리는게 아니라 public ip를 넣는다.
        myConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.0.147:9092, 172.31.4.6:9092, 172.31.13.150:9092");
        myConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        myConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(myConfig);
    }

    @Bean
    public ConsumerFactory<String, Object> ConsumerFactory() {
        Map<String, Object> myConfig = new HashMap<>();

        myConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.0.147:9092, 172.31.4.6:9092, 172.31.13.150:9092");
        myConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        myConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(myConfig);
    }

    // 주요 역할
    // 1. Kafka 메시지 리스너 설정
    // kafkaListenerContainerFactory는 Kafka 메시지를 비동기적으로 수신할 리스너를 생성하고 설정하는 데 사용된다.
    // Kafka 브로커로부터 메시지를 수신할 컨테이너를 관리한다.
    // Spring Kafka는 @KafkaListener를 기반으로 메시지 리스너 컨테이너를 생성하는데, 이 컨테이너를 만들기 위해 kafkaListenerContainerFactory를 사용한다.
    // 리스너 컨테이너는 Kafka 소비자(Consumer)의 실행 환경을 제공한다 - 병렬 처리를 위한 스레드 관리, 오류 처리, 메시지 변환 등을 설정할 수 있다.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {

        // 2. 동시성 지원
        // ConcurrentKafkaListenerContainerFactory는 여러 쓰레드에서 동시에 메시지를 처리할 수 있도록 설계된 리스너 컨테이너를 제공한다.
        // 이를 통해 높은 메시지 처리량을 처리할 수 있다.
        ConcurrentKafkaListenerContainerFactory<String, Object> myfactory = new ConcurrentKafkaListenerContainerFactory<>();

        // 3. 메시지를 가져오는 데 사용할 Kafka Consumer를 설정한다.
        // 이 ConsumerFactory()는 Kafka 클라이언트의 소비자(consumer) 설정을 정의한 팩토리이다.
        // ConsumerFactory를 지정해 주는 것이다. 그러면 그 리스너가 그 ConsumerFactory를 사용해서 concurrent하게 데이터를 가져온다.
        myfactory.setConsumerFactory(ConsumerFactory());
        return myfactory;
    }
}
