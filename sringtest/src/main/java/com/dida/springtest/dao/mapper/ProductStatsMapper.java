package com.dida.springtest.dao.mapper;

import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

/**
 * @Author：zhd
 * @Date: 2022/5/4 11:30
 * @Dscription: 商品金额Mapper映射
 */
public interface ProductStatsMapper {

    //获取商品交易额
    @Select("select sum(order_amount) order_amount from product_stats_2021 where toYYYYMMDD(stt)=#{date}")
    public BigDecimal getGMV(int date);

}
