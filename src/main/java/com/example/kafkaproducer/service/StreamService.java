package com.example.kafkaproducer.service;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StreamService {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    public void buildPipeline(StreamsBuilder sb) {

        // java stream과 비슷하다고 생각하면 된다.
            // key:value --> 메시지를 카프카에 프로듀서할 때 key로 여러가지를 할 수 있다.
        KStream<String, String> myStream = sb.stream("defaultTopic", Consumed.with(STRING_SERDE, STRING_SERDE));
        myStream.print(Printed.toSysOut());
        // freeClass라는 단어가 포함된 데이터를 필터링해서 freeClassList라는 토픽으로 보낸다.
        myStream.filter((key, value)-> value.contains("freeClass")).to("freeClassList");
        // 결과적으로 defaultTopic에 있는 데이터를 읽어와서 freeClass라는 단어가 포함된 데이터만 필터링해서 freeClassList라는 토픽으로 보낸다.
    }
}
