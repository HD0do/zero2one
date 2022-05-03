package com.xiaoshouyi.oneservice.service;

/**
 * @Author：zhd
 * @Date: 2021/10/27 11:37
 * @Dscription:
 */

import java.math.BigDecimal;

/**
 * 商品金额统计接口
 */
public interface ProductTotalAmountService {
    //获取某一天的总交易额
    public BigDecimal getAmount(int date);
}
