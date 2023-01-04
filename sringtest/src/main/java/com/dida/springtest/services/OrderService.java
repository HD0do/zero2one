package com.dida.springtest.services;

import java.math.BigDecimal;

/**
 * @Author：zhd
 * @Date: 2023/1/3 16:58
 * @Dscription:
 */
public interface OrderService {


    //获取指定日期的GMV
    public BigDecimal getGMV(String date) ;



}
