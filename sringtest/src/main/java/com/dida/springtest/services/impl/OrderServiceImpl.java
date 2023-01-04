package com.dida.springtest.services.impl;

import com.dida.springtest.dao.mapper.OrderMapper;
import com.dida.springtest.services.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.math.BigDecimal;

/**
 * @Author：zhd
 * @Date: 2023/1/3 16:59
 * @Dscription:
 */
@Service
public class OrderServiceImpl implements OrderService {

    @Autowired
    OrderMapper orderMapper;

    @Override
    public BigDecimal getGMV(String date) {
        return null;
    }
}
