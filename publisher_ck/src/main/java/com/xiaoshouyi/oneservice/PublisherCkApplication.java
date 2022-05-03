package com.xiaoshouyi.oneservice;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@MapperScan(basePackages = "com.xiaoshouyi.oneservice.mapper")
@SpringBootApplication
public class PublisherCkApplication {

    public static void main(String[] args) {
        SpringApplication.run(PublisherCkApplication.class, args);
    }

}
