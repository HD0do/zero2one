package com.dida.springtest;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
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
 *
 * 访问者设计模式，AST结点研究、递归遍历、druid、二叉树、链表
 * @Dscription: 参考：https://blog.csdn.net/cckevincyh/article/details/125317977
 *                  ：https://www.cnblogs.com/zedW/articles/15921991.html
 *                  ：https://gitee.com/haitao/data-lineage-parent/tree/master
 *                  ：https://blog.csdn.net/Message_lx/article/details/125858132
 *                  ：https://blog.csdn.net/cckevincyh/article/details/125317977 （*很不错的文章：讲解了二叉树的不同遍历方式及AST特点，通过遍历获取各个结点）
 */



public class SqlParse {

    @Test
    public void sqlParse(){


        String sql = "insert into table member SELECT t.id ,a.member_name,c.test FROM user as t join order a on t.id=a.member_id  join (select test,order_id,tag_id from item) c on a.id=c.order_id WHERE  t.status = 1";

        DbType dbType = JdbcConstants.MYSQL;
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, dbType);
        System.out.println(statementList.get(0).getDbType());




        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        SQLStatement sqlStatement = parser.parseStatement();
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, dbType);

        SchemaStatVisitor visitor1 = new SchemaStatVisitor();
        System.out.println(sqlStatements.size());
        for (SQLStatement stmt : sqlStatements) {
            stmt.accept(visitor1);
            System.out.println("-------------");
            System.out.println(visitor1.getColumns());
            System.out.println("-------------");
        }

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
