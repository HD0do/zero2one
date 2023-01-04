package com.dida.springtest.controller;

import com.dida.springtest.services.OrderService;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

/**
 * @Author：zhd
 * @Date: 2022/12/28 14:42
 * @Dscription:
 */

@Api("订单查询")
@RestController
@RequestMapping("/order")
public class OrderController {

    @Autowired
    OrderService orderService;

    SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd");


    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") String date) {
        if( "0".equals(date)){
            date = sdf.format(System.currentTimeMillis());
        }
        BigDecimal gmv = orderService.getGMV(date);
        String json = "{   \"status\": 0,  \"data\":" + gmv + "}";
        System.out.println(json);
        return  json;
    }

}
