package com.xiaoshouyi.oneservice.mapper;

/**
 * @Author：zhd
 * @Date: 2021/10/27 11:28
 * @Dscription:
 */

import org.apache.ibatis.annotations.Select;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.math.BigDecimal;

/**
 * Desc:商品销售额统计Mapper
 */
public interface ProductTotalAmountMapper {
    //获取商品交易额
    @Select("select sum(order_amount) order_amount  " +
            "from product_stats_2021 where toYYYYMMDD(stt)=#{date}")
    public BigDecimal getAmount(int date);
}
