package com.imooc.stream.map;

import com.youfan.entity.KafkaMessage;
import org.apache.flink.api.common.functions.RichMapFunction;

public class PindaopvuvMap extends RichMapFunction<KafkaMessage,KafkaMessage> {
    @Override
    public KafkaMessage map(KafkaMessage kafkaMessage) throws Exception {
        return null;
    }
}
