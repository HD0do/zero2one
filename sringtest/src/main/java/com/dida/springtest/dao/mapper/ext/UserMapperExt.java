package com.dida.springtest.dao.mapper.ext;

import com.dida.springtest.dao.mapper.UserMapper;
import org.apache.ibatis.annotations.Param;

/**
 * @Author：zhd
 * @Date: 2023/1/5 17:43
 * @Dscription:
 */
public interface UserMapperExt extends UserMapper {

    Integer selectCountByOriginSql(@Param("sql") String countSql);

}
