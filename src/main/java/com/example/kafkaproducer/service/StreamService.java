package com.example.kafkaproducer.service;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class StreamService {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    public void buildPipeline(StreamsBuilder sb) {

        // 1. filtering 기능 실습
        // java stream과 비슷하다고 생각하면 된다.
            // key:value --> 메시지를 카프카에 프로듀서할 때 key로 여러가지를 할 수 있다.
//        KStream<String, String> myStream = sb.stream("defaultTopic", Consumed.with(STRING_SERDE, STRING_SERDE));
//        myStream.print(Printed.toSysOut());
//        // freeClass라는 단어가 포함된 데이터를 필터링해서 freeClassList라는 토픽으로 보낸다.
//        myStream.filter((key, value)-> value.contains("freeClass")).to("freeClassList");
        // 결과적으로 defaultTopic에 있는 데이터를 읽어와서 freeClass라는 단어가 포함된 데이터만 필터링해서 freeClassList라는 토픽으로 보낸다.

        // 2. join 기능 실습
        KStream<String, String> leftStream = sb.stream("leftTopic", Consumed.with(STRING_SERDE, STRING_SERDE));
//        // key:value --> 1:leftValue
        KStream<String, String> rightStream = sb.stream("rightTopic", Consumed.with(STRING_SERDE, STRING_SERDE));
//        // key:value --> 1:rightValue

        ValueJoiner<String, String, String> stringJoiner = (leftValue, rightValue) -> {
            return "[StringJoiner]" + leftValue + "-" + rightValue;
        };

        ValueJoiner<String, String, String> stringOuterJoiner = (leftValue, rightValue) -> {
            return "[StringOuterJoiner]" + leftValue + "<" + rightValue;
        };

        KStream<String, String> joinedStream = leftStream.join(rightStream,
                stringJoiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)));

        KStream<String, String> outerJoinedStream = leftStream.outerJoin(rightStream,
                stringOuterJoiner,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)));

        joinedStream.print(Printed.toSysOut());
        joinedStream.to("joinedMsg");
        outerJoinedStream.to("joinedMsg");
    }
}
