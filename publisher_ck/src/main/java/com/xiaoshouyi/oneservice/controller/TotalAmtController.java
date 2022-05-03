package com.xiaoshouyi.oneservice.controller;

import com.xiaoshouyi.oneservice.service.ProductTotalAmountService;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @Author：zhd
 * @Date: 2021/10/27 11:55
 * @Dscription: 主要接收用户请求，并做出相应，返回不同的格式
 */

@RestController
@RequestMapping("/api")
public class TotalAmtController {

    @RequestMapping("/hello")
    public String handle01(){
        return "hello world springBoot";
    }

    @Autowired
    ProductTotalAmountService productTotalAmountService;

    @RequestMapping("/totalAmt")
    public String getTotalAmt(@RequestParam(value = "date" ,defaultValue = "0")Integer date){
        if (date == 0){
            date=now();
    }
        BigDecimal totalAmt = productTotalAmountService.getAmount(date);
        String json ="{   \"status\": 0,  \"data\":" + totalAmt + "}";
        return json;
    }

    private int now() {

        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return   Integer.valueOf(yyyyMMdd);

    }

}
