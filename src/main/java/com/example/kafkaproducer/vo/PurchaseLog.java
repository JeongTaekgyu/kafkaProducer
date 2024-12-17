package com.example.kafkaproducer.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.Map;

@Data
public class PurchaseLog {
    String orderId; //  od-0001
    String userId; // uid-0001
    //ArrayList<String> productId; // {pg-0001, pg-0002}
    ArrayList<Map<String, String>> productInfo;  // [ {"productid":"pg-0001", "price":"24000"}, {}... ]
    String purchasedDt; // 20230201070000
    //Long price; // 24000
}
