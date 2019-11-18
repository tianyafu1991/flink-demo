package com.imooc.stream.map;

import com.alibaba.fastjson.JSON;
import com.youfan.analy.PindaoRD;
import com.youfan.entity.KafkaMessage;
import com.youfan.entity.UserscanLog;
import org.apache.flink.api.common.functions.RichMapFunction;

public class PingdaoKafkaMap extends RichMapFunction<KafkaMessage, PindaoRD> {
    @Override
    public PindaoRD map(KafkaMessage kafkaMessage) throws Exception {
        String jsonmessage = kafkaMessage.getJsonmessage();
        System.out.println("Map接收到数据"+jsonmessage);
        UserscanLog userscanLog = JSON.parseObject(jsonmessage, UserscanLog.class);
        Long pingdaoid = userscanLog.getPingdaoid();
        PindaoRD pindaoRD = new PindaoRD();
        pindaoRD.setPingdaoid(pingdaoid);
        pindaoRD.setCount(Long.parseLong(kafkaMessage.getCount()+""));

        return pindaoRD;
    }
}
