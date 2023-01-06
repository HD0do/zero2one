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
    @Select("select sum(b.deal_sub_amt) as order_amount from dev_ods.ods_order_db_order_header_ms a join dev_ods.ods_order_db_order_item_ms b\n" +
            "on a.id = b.order_id")
    public BigDecimal getGMV(String date);



}
