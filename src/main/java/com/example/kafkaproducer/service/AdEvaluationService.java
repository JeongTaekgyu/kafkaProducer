package com.example.kafkaproducer.service;

import com.example.kafkaproducer.vo.EffectOrNot;
import com.example.kafkaproducer.vo.PurchaseLog;
import com.example.kafkaproducer.vo.PurchaseLogOneProduct;
import com.example.kafkaproducer.vo.WatchingAdLog;
import lombok.Data;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Service
public class AdEvaluationService {
    // 광고를 여러번 볼 때마다 log를 남길 필요가 없기 때문에 (물론 여러번 보는 것도 의미가 있긴 한데 여기서는 그냥 한 번 보는 것만 체크한다.)
    // 광고 데이터 중복 Join될 필요없다. --> Table
    // 광고 이력이 먼저 들어옵니다.
    // 구매 이력은 상품별로 들어오지 않는다. (복수개의 상품 존재) --> contain
    // 광고에 머문시간이 10초 이상되어야만 join 대상
    // 특정가격이상의 상품은 join 대상에서 제외 (100만원)
    // 광고이력 : KTable(AdLog), 구매이력 : KTable(PurchaseLogOneProduct)
    // filtering, 형 변환,
    // EffectOrNot --> Json 형태로 Topic : AdEvaluationComplete

    Producer myprdc;

    @Autowired
    public void buildPipeline(StreamsBuilder sb) {

        // 1. 직렬화 및 역직렬화 설정
        //object의 형태별로 Serializer, Deserializer 를 설정합니다.
        JsonSerializer<EffectOrNot> effectSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLog> purchaseLogSerializer = new JsonSerializer<>();
        JsonSerializer<WatchingAdLog> watchingAdLogSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLogOneProduct> purchaseLogOneProductSerializer = new JsonSerializer<>();

        JsonDeserializer<EffectOrNot> effectDeserializer = new JsonDeserializer<>(EffectOrNot.class);
        JsonDeserializer<PurchaseLog> purchaseLogDeserializer = new JsonDeserializer<>(PurchaseLog.class);
        JsonDeserializer<WatchingAdLog> watchingAdLogJsonDeserializer = new JsonDeserializer<>(WatchingAdLog.class);
        JsonDeserializer<PurchaseLogOneProduct> purchaseLogOneProductDeserializer = new JsonDeserializer<>(PurchaseLogOneProduct.class);

        Serde<EffectOrNot> effectOrNotSerde = Serdes.serdeFrom(effectSerializer, effectDeserializer);
        Serde<PurchaseLog> purchaseLogSerde = Serdes.serdeFrom(purchaseLogSerializer, purchaseLogDeserializer);
        Serde<WatchingAdLog> watchingAdLogSerde = Serdes.serdeFrom(watchingAdLogSerializer, watchingAdLogJsonDeserializer);
        Serde<PurchaseLogOneProduct> purchaseLogOneProductSerdeSerde = Serdes.serdeFrom(purchaseLogOneProductSerializer, purchaseLogOneProductDeserializer);

        // 2. adLog 스트림을 테이블로 변환
        // adLog 토픽의 광고 데이터를 Kafka 스트림에서 읽어오고, userId와 productId를 결합한 키를 설정하여 KTable로 변환한다.
        // 이 테이블은 광고 이력을 저장하고 관리하며, 중복 데이터 없이 광고 기록을 처리할 수 있다
        // adLog topic 을 consuming 하여 KTable 로 받는다. (광고는 데이터 중복 Join이 필요없다. 그러므로 Table을 사용한다.)
        KTable<String, WatchingAdLog> adTable = sb.stream("adLog", Consumed.with(Serdes.String(), watchingAdLogSerde))
                .selectKey((k,v) -> v.getUserId() + "_" + v.getProductId()) // key를 userId+prodId로 생성해줍니다.
                .filter((k,v)-> Integer.parseInt(v.getWatchingTime()) > 10) // 광고 시청시간이 10초 이상인 데이터만 Table에 담습니다.
                .toTable(Materialized.<String, WatchingAdLog, KeyValueStore<Bytes, byte[]>>as("adStore") // Key-Value Store로 만들어줍니다.
                        .withKeySerde(Serdes.String())
                        .withValueSerde(watchingAdLogSerde)
                );

        // 3. purchaseLog 스트림처리
        // 구매 로그(purchaseLog) 스트림을 처리하여 여러 제품 정보 중에서 특정 가격(100만원 이하)의 제품만 처리하도록 필터링한다.
        // 필터링된 제품 정보는 PurchaseLogOneProduct 객체로 변환되어 purchaseLogOneProduct 토픽으로 전송된다.
        // foreach 문을 사용해 제품 목록을 순회하며, 각 제품이 필터링 조건을 충족하면 새로운 구매 로그 객체를 생성해 전송하는 구조다.
        KStream<String, PurchaseLog> purchaseLogKStream = sb.stream("purchaseLog", Consumed.with(Serdes.String(), purchaseLogSerde));
                // purchaseLog topic 을 consuming 하여 KStream 으로 받는다.  stream("topic", Consumed.with(key, value) )

        // 해당 KStream의 매 Row(Msg)마다 아래 내용을 수행한다.
        purchaseLogKStream.foreach((k,v) -> {
                // value 의 product 개수만큼 반복하여 신규 VO에 값을 Binding 한다.
                for (Map<String, String> prodInfo:v.getProductInfo())   {
                    // price 100000 미만인 경우만 아래 내용을 수행한다. 위 purchaseLogKStream에서 filter조건을 주고 받아도 무관하다.
                    if (Integer.parseInt(prodInfo.get("price")) < 1000000) {
                        PurchaseLogOneProduct tempVo = new PurchaseLogOneProduct();
                        tempVo.setUserId(v.getUserId());
                        tempVo.setProductId(prodInfo.get("productId"));
                        tempVo.setOrderId(v.getOrderId());
                        tempVo.setPrice(prodInfo.get("price"));
                        tempVo.setPurchasedDt(v.getPurchasedDt());

                        // 1개의 product로 나눈 데이터를 purchaseLogOneProduct Topic으로 produce 합니다.
                        myprdc.sendJoinedMsg("purchaseLogOneProduct", tempVo);

                        // 하기의 method는 samplie Data를 생산하여 Topic에 넣습니다. 1개 받으면 여러개를 생성하기 때문에 무한하게 생성됩니다. .
                        // 일단 주석을 해놓는다.
                        // sendNewMsg();
                    }
                }
            }
        );

        // 4. purchaseLogOneProduct 스트림을 테이블로 변환
        // purchaseLogOneProduct 토픽에서 데이터를 읽어와, userId와 productId를 키로 사용하여 KTable로 변환한다.
        // 이 테이블은 각 제품별 구매 기록을 관리하며, 광고 기록과 결합(join)할 준비를 한다
        // product이 1개씩 나누어 producing 된 topic을 읽어 KTable 로 받아옵니다.
        KTable<String, PurchaseLogOneProduct> purchaseLogOneProductKTable= sb.stream("purchaseLogOneProduct", Consumed.with(Serdes.String(), purchaseLogOneProductSerdeSerde))
                // Stream의 key 지정
                .selectKey((k,v)-> v.getUserId()+ "_" +v.getProductId())
                // key-value Store로 이용이 가능하도록 생성
                .toTable(Materialized.<String, PurchaseLogOneProduct, KeyValueStore<Bytes, byte[]>>as("purchaseLogStore")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(purchaseLogOneProductSerdeSerde)
                );

        // 5. join 로직
        // value joiner 를 통해 Left, Right 값을 통한 Output 결과값을 bind 하거나 join 조건을 설정할 수 있다.
        // ValueJoiner를 사용해 광고 로그(WatchingAdLog)와 구매 로그(PurchaseLogOneProduct)를 조인한다.
        // 두 테이블을 조인하여 EffectOrNot 객체를 생성하며, 광고가 실제로 구매로 이어졌는지에 대한 데이터를 포함한다.
        // 조인된 데이터는 userId, adId, orderId와 제품 정보를 포함하며, 이를 통해 광고 효과를 평가할 수 있다
        ValueJoiner<WatchingAdLog, PurchaseLogOneProduct, EffectOrNot> tableStreamJoiner = (leftValue, rightValue) -> {
            EffectOrNot returnValue = new EffectOrNot();
            returnValue.setUserId(rightValue.getUserId());
            returnValue.setAdId(leftValue.getAdId());
            returnValue.setOrderId(rightValue.getOrderId());
            Map<String, String> tempProdInfo = new HashMap<>();
            tempProdInfo.put("productId", rightValue.getProductId());
            tempProdInfo.put("price", rightValue.getPrice());
            returnValue.setProductInfo(tempProdInfo);
            System.out.println("Joined!");
            return returnValue;
        };

        // 6. 결과 전송
        // 조인된 데이터를 toStream()을 통해 스트림으로 변환하고, AdEvaluationComplete라는 Kafka 토픽으로 전송한다.
        // 이 토픽은 최종적으로 광고 효과가 있는지 여부를 담은 데이터를 전달하는 역할을 한다.
        // table과 joiner를 입력해준다. stream join이 아니기에 window 설정은 불필요하다.
        adTable.join(purchaseLogOneProductKTable,tableStreamJoiner)
                // join 이 완료된 데이터를 AdEvaluationComplete Topic으로 전달합니다.
                .toStream().to("AdEvaluationComplete", Produced.with(Serdes.String(), effectOrNotSerde));
    }
    // 결론은
    // 테이블을 사용하여 중복된 광고 로그를 방지하고, 실시간으로 데이터를 처리함으로써 광고 효과를 빠르게 평가할 수 있다.
    // 사용자가 광고를 본 제품을 구매할 경우, 광고가 효과적이었음을 판단하고 광고의 효과 비율을 분석한다.

    public void sendNewMsg() {
        PurchaseLog tempPurchaseLog  = new PurchaseLog();
        WatchingAdLog tempWatchingAdLog = new WatchingAdLog();

        //랜덤한 ID를 생성하기 위해 아래의 함수를 사용합니다.
        // random Numbers for concatenation with attrs
        Random rd = new Random();
        int rdUidNumber = rd.nextInt(9999);
        int rdOrderNumber = rd.nextInt(9999);
        int rdProdIdNumber = rd.nextInt(9999);
        int rdPriceIdNumber = rd.nextInt(90000)+10000;
        int prodCnt = rd.nextInt(9)+1;
        int watchingTime = rd.nextInt(55)+5;

        // bind value for purchaseLog
        tempPurchaseLog.setUserId("uid-" + String.format("%05d", rdUidNumber));
        tempPurchaseLog.setPurchasedDt("20230101070000");
        tempPurchaseLog.setOrderId("od-" + String.format("%05d", rdOrderNumber));
        ArrayList<Map<String, String>> tempProdInfo = new ArrayList<>();
        Map<String, String> tempProd = new HashMap<>();
        for (int i=0; i<prodCnt; i++ ){
            tempProd.put("productId", "pg-" + String.format("%05d", rdProdIdNumber));
            tempProd.put("price", String.format("%05d", rdPriceIdNumber));
            tempProdInfo.add(tempProd);
        }
        tempPurchaseLog.setProductInfo(tempProdInfo);

        // bind value for watchingAdLog
        tempWatchingAdLog.setUserId("uid-" + String.format("%05d", rdUidNumber));
        tempWatchingAdLog.setProductId("pg-" + String.format("%05d", rdProdIdNumber));
        tempWatchingAdLog.setAdId("ad-" + String.format("%05d",rdUidNumber) );
        tempWatchingAdLog.setAdType("banner");
        tempWatchingAdLog.setWatchingTime(String.valueOf(watchingTime));
        tempWatchingAdLog.setWatchingDt("20230201070000");

        // produce msg
        myprdc.sendMsgForPurchaseLog("purchaseLog", tempPurchaseLog);
        myprdc.sendMsgForWatchingAdLog("adLog", tempWatchingAdLog);
    }
}
