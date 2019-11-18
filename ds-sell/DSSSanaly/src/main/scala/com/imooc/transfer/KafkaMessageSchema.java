package com.imooc.transfer;

import com.alibaba.fastjson.JSON;
import com.youfan.entity.KafkaMessage;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class KafkaMessageSchema implements DeserializationSchema<KafkaMessage> , SerializationSchema<KafkaMessage> {
    /**
     * 反序列化
     * @param bytes
     * @return
     * @throws IOException
     */
    @Override
    public KafkaMessage deserialize(byte[] bytes) throws IOException {
        KafkaMessage kafkaMessage = JSON.parseObject(new String(bytes), KafkaMessage.class);
        return kafkaMessage;
    }

    /**
     * 序列化
     * @param kafkaMessage
     * @return
     */
    @Override
    public byte[] serialize(KafkaMessage kafkaMessage) {
        String jsonString = JSON.toJSONString(kafkaMessage);
        return jsonString.getBytes();
    }

    /**
     * 判断是否是流的末端
     * @param kafkaMessage
     * @return
     */
    @Override
    public boolean isEndOfStream(KafkaMessage kafkaMessage) {
        return false;
    }

    /**
     * 返回类型信息
     * @return
     */
    @Override
    public TypeInformation<KafkaMessage> getProducedType() {
        return TypeInformation.of(KafkaMessage.class);
    }
}
