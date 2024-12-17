package com.example.kafkaproducer.service;

import com.example.kafkaproducer.config.KafkaConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {

    @Autowired
    KafkaConfig myConfig;

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

    public void sendJoinedMsg (String topicNm, Object msg) {
        kafkaTemplate = myConfig.KafkaTemplateForGeneral();
        kafkaTemplate.send(topicNm, msg);
    }
    public void sendMsgForWatchingAdLog (String topicNm, Object msg) {
        kafkaTemplate = myConfig.KafkaTemplateForWatchingAdLog();
        kafkaTemplate.send(topicNm, msg);
    }

    public void sendMsgForPurchaseLog (String topicNm, Object msg) {
        kafkaTemplate = myConfig.KafkaTemplateForPurchaseLog();
        kafkaTemplate.send(topicNm, msg);
    }
}
