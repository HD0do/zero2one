package com.xiaoshouyi.oneservice.service.impl;

import com.xiaoshouyi.oneservice.mapper.ProductTotalAmountMapper;
import com.xiaoshouyi.oneservice.service.ProductTotalAmountService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * @Author：zhd
 * @Date: 2021/10/27 11:44
 * @Dscription:商品统计接口实现类
 */

@Service
public class ProductTotalAmoutServiceimpl implements ProductTotalAmountService {

    @Autowired
    ProductTotalAmountMapper productTotalAmountMapper;

    @Override
    public BigDecimal getAmount(int date) {
        return productTotalAmountMapper.getAmount(date);
    }
}
