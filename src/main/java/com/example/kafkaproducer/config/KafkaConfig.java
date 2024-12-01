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

    // Kafka 브로커로부터 메시지를 수신할 컨테이너를 관리한다.
    // ConsumerFactory를 지정해 주는 것이다. 그러면 그 리스너가 그 ConsumerFactory를 사용해서 concurrent하게 데이터를 가져온다.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> myfactory = new ConcurrentKafkaListenerContainerFactory<>();
        myfactory.setConsumerFactory(ConsumerFactory());
        return myfactory;
    }
}
