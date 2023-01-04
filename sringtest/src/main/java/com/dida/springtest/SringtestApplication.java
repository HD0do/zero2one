package com.dida.springtest;

import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceAutoConfigure;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(exclude = DruidDataSourceAutoConfigure.class)
@MapperScan(basePackages = "com.dida.springtest.dao.mapper")
public class SringtestApplication {

    public static void main(String[] args) {
        //代码初始化入口
        SpringApplication.run(SringtestApplication.class, args);

    }

}
