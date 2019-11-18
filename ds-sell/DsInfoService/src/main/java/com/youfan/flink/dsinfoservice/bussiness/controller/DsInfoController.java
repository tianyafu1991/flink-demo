package com.youfan.flink.dsinfoservice.bussiness.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.youfan.entity.KafkaMessage;
import com.youfan.entity.UserscanLog;
import com.youfan.flink.dsinfoservice.bussiness.data.Scanlogproduce;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Date;

@RestController
@RequestMapping(value = "/dsInfo")
public class DsInfoController {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    //localhost:6097/dsInfo/webInfoSJService?jsonstr=23
    @RequestMapping(value = "/webInfoSJService",method = RequestMethod.POST)
    public void webInfoSJService(@RequestBody String jsonstr, HttpServletRequest request, HttpServletResponse response){
        System.out.println("hello jin来了======="+jsonstr);

        KafkaMessage kafkaMessage = new KafkaMessage();
        kafkaMessage.setCount(1);
        kafkaMessage.setTimestamp(new Date().getTime());
        kafkaMessage.setJsonmessage(jsonstr);
        jsonstr= JSONObject.toJSONString(kafkaMessage);

        //业务开始
        kafkaTemplate.send("flink-kafka-msi","key",jsonstr);
        //业务结束
        PrintWriter writer = getWriter(response);

        response.setStatus(HttpStatus.OK.value());
        writer.write("success");
        closeprintwriter(writer);
    }

    private PrintWriter getWriter(HttpServletResponse response){
        response.setCharacterEncoding("utf-8");
        response.setContentType("application/json");
        OutputStream out = null;
        PrintWriter printWriter = null;
        try {
            out = response.getOutputStream();
            printWriter = new PrintWriter(out);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return printWriter;
    }

    private void closeprintwriter(PrintWriter printWriter){
        printWriter.flush();
        printWriter.close();
    }



}
