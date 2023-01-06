package com.dida.springtest;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import com.mysql.cj.xdevapi.InsertStatement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @Author：zhd
 * @Date: 2023/1/6 16:22
 * @Dscription: 参考：https://blog.csdn.net/cckevincyh/article/details/125317977
 */



public class SqlParse {

    @Test
    public void sqlParse(){


        String sql = "insert into table member SELECT t.id，a.member_name,c.test FROM user as t join order a on t.id=a.member_id  join (select test,order_id,tag_id from item) c on a.id=c.order_id WHERE  t.status = 1";

        DbType dbType = JdbcConstants.MYSQL;
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, dbType);
        System.out.println(statementList.get(0).getDbType());




        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        SQLStatement sqlStatement = parser.parseStatement();



        SchemaStatVisitor visitor1 = new SchemaStatVisitor();
        MySqlInsertStatement insert = (MySqlInsertStatement) sqlStatement;
        insert.accept(visitor1);



        System.out.println(insert.getQuery().getFirstQueryBlock().getSelectList());

        SchemaStatVisitor visitor = new SchemaStatVisitor();
        sqlStatement.accept(visitor);



        Collection<TableStat.Column> columns = visitor.getColumns();
        System.out.println(columns);

        System.out.println(visitor.getTables());
        System.out.println(visitor.getConditions());


        List<String> columnList = new ArrayList<>();
        columns.stream().forEach(row -> {
            if(row.isSelect()){
                //保存select字段
                columnList.add(row.getTable()+"."+row.getName());
            }
        });

        System.out.println(columnList.toString());

    }

}
