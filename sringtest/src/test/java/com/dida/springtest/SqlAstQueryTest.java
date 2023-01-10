package com.dida.springtest;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.util.JdbcConstants;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * @Author：zhd
 * @Date: 2023/1/9 11:13
 * @Dscription:
 */
public class SqlAstQueryTest {

    @Test
    public void sqlQueryTest(){
        String sql = " SELECT t.id，a.member_name as aaa,c.test FROM user as t join order a on t.id=a.member_id  join (select test,order_id,tag_id from item) c on a.id=c.order_id WHERE  t.status = 1";

        List<SQLStatement> statements = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);

        SQLStatement statement = statements.get(0);

        SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) statement;
        SQLSelectQuery     sqlSelectQuery     = sqlSelectStatement.getSelect().getQuery();
        SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;

        SQLTableSource table = sqlSelectQueryBlock.getFrom();

        System.out.println(table.toString());

        List<SQLSelectItem> selectItems         = sqlSelectQueryBlock.getSelectList();

        selectItems.forEach(x ->{
//            System.out.println(x.toString());
            System.out.println(x.getAlias());
                }


        );


    }
}
