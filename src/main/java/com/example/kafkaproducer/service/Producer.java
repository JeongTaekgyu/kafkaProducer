package com.example.kafkaproducer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {

    // publish를 하려면 topic이 있어야한다.
    String topicName = "defaultTopic";

    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public Producer(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // 실제 우리가 구현할 메시지
    public void pub(String msg) {
        kafkaTemplate.send(topicName, msg);
    }
}
