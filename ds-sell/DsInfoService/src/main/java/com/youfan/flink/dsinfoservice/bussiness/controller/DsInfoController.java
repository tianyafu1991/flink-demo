package com.youfan.flink.dsinfoservice.bussiness.controller;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

@RestController
@RequestMapping(value = "/dsInfo")
public class DsInfoController {

    //localhost:6097/dsInfo/webInfoSJService?jsonstr=23
    @RequestMapping(value = "/webInfoSJService",method = RequestMethod.POST)
    public void webInfoSJService(@RequestBody String jsonstr, HttpServletRequest request, HttpServletResponse response){
        System.out.println("hello jin来了======="+jsonstr);
        //业务开始

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
