package com.wn.spark;

import java.util.HashMap;
import java.util.Map;


public class KafkaParamBuilder {

    private Map<String, String> kafkaParamMap = new HashMap<String, String>(12);

    public KafkaParamBuilder brokerAddress(){
        kafkaParamMap.put("metadata.broker.list", "server3:9092");
        kafkaParamMap.put("fetch.message.max.bytes", "104857600");
        return this;
    }

    public KafkaParamBuilder group(String group) {
        kafkaParamMap.put("group.id", group);
        return this;
    }

    public Map<String, String> toBuilder() {
        return kafkaParamMap;
    }
}
