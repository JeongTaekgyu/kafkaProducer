package com.example.kafkaproducer.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    // 참고로 topic을 받을때 컨슈머 그룹안에 컨슈머가 있다.
    /*@KafkaListener(topics = "defaultTopic", groupId = "spring")
    public void consumer(String message) {
        System.out.println(String.format("Subscribed message: %s" + message));
    }*/
}
