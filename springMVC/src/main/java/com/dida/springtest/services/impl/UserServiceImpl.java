package com.dida.springtest.services.impl;

import com.dida.springtest.dao.mapper.ext.UserMapperExt;
import com.dida.springtest.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Author：zhd
 * @Date: 2023/1/6 10:02
 * @Dscription:
 */
@Service
public class UserServiceImpl implements UserService {

    @Autowired
    UserMapperExt userMapperExt;


    @Override
    public Integer getUserMum(String sql) {
        return userMapperExt.selectCountByOriginSql(sql);
    }
}
