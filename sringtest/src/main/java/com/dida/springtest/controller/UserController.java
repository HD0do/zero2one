package com.dida.springtest.controller;

import com.dida.springtest.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

/**
 * @Author：zhd
 * @Date: 2023/1/6 10:04
 * @Dscription:
 */
@RestController
@RequestMapping("/user")
public class UserController {

    @Autowired
    UserService userService;

    @RequestMapping("/a")
    public String getNum() {

      long nums = userService.getUserMum("select count(1) from dev_ods.ods_order_db_order_header_ms");

        String json = "{\"status\": 0,  \"data\":" + nums + "}";
        System.out.println(json);


        return json;

    }
}
