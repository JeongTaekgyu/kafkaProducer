package com.example.kafkaproducer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
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
        myConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.0.123:9092, 172.31.4.62:9092, 172.31.13.550");
        myConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        myConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(myConfig);
    }

}
