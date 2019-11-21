package com.youfan.dsservicecenterserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

@SpringBootApplication
@EnableEurekaServer
public class DsServiceCenterServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DsServiceCenterServerApplication.class, args);
    }

}
