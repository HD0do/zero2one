package com.dida.springtest.core.sql;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

/**
 * @Author：zhd * @Date: 2023/1/6 10:55
 * @Dscription:
 */
public class SQLParse {

    String sql = "SELECT id FROM user WHERE status = 1";

    DbType dbType = JdbcConstants.MYSQL;

    List<SQLStatement> statementList = SQLUtils.parseStatements(sql, dbType);

}
