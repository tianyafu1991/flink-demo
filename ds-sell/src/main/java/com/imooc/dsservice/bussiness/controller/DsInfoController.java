package com.imooc.dsservice.bussiness.controller;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping(value = "/info")
public class DsInfoController {

    // localhost:6097/info/DsInfoController
    @RequestMapping(value = "/DsInfoController",method = RequestMethod.POST)
    public void webInfoCollectService(@RequestBody String jsonStr, HttpServletRequest request, HttpServletResponse response){
        System.out.println("hello 我来了");
    }


}
