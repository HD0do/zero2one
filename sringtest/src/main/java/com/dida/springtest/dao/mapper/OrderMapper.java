package com.dida.springtest.dao.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/**
 * @Author：zhd
 * @Date: 2022/5/4 11:30
 * @Dscription: 商品金额Mapper映射
 */
public interface OrderMapper {

    //获取商品交易额
    @Select("select sum(deal_amt) as order_amount from dev_ods.ods_order_db_order_header_ms")
    public BigDecimal getGMV(String date);



}
