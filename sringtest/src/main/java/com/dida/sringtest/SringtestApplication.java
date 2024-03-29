package com.dida.sringtest;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.dida.gmall.mapper")
public class SringtestApplication {

    public static void main(String[] args) {
        //代码初始化入口
        SpringApplication.run(SringtestApplication.class, args);
    }

}
