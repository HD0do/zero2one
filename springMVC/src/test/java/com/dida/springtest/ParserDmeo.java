package com.dida.springtest;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;

import java.util.*;

/**
 * Created by LH on 2022/9/2 17:43
 * 遍历获取最外层的数据字段
 * 参考：https://blog.csdn.net/lilyjoke/article/details/126679248
 */
public class ParserDmeo{
    //原始表字段
    public static HashMap<String, List<String>> originalTables = new HashMap<String, List<String>>();

    //用于存储最终输出的字段
    public static List<OutColumn> outColumns = new ArrayList<OutColumn>();
    public static class OutColumn{
        String columnName;
        String columnAlias;
        public OutColumn(String columnName, String columnAlias) {
            this.columnName = columnName;
            this.columnAlias = columnAlias;
        }

        @Override
        public String toString() {
            return "OutColumn{" +
                    "columnName='" + columnName + '\'' +
                    ", columnAlias='" + columnAlias + '\'' +
                    '}';
        }

    }
    public static void main(String[] args) throws Exception {
        List<String> columna = new ArrayList<String>();
        columna.add("id");
        columna.add("name");
        columna.add("phone");
        originalTables.put("tb_a", columna);

        List<String> columnb = new ArrayList<String>();
        columnb.add("id");
        columnb.add("address");
        columnb.add("cost");
        originalTables.put("tb_b", columnb);

        List<String> columnc = new ArrayList<String>();
        columnc.add("id");
        columnc.add("level");
        columnc.add("email");
        originalTables.put("tb_c", columnc);


        String sql = "select a.*, b.*, mon,tb_c.email from tb_a a " +
                "join (select id, sum(cost) as mon from ( select * from tb_b) bb group by id) b" +
                " on a.id = b.id " +
                "join tb_c on tb_c.id = tb_a.id ";

        SQLStatement sqlStatement = SQLUtils.parseSingleStatement(sql, DbType.hive, false);
        if (sqlStatement instanceof SQLSelectStatement
                && ((SQLSelectStatement) sqlStatement).getSelect() != null
                && ((SQLSelectStatement) sqlStatement).getSelect().getQuery() != null) {
            SQLSelectQuery select = ((SQLSelectStatement) sqlStatement).getSelect().getQuery();
            //总体查询分两种，select和union
            if (select instanceof SQLSelectQueryBlock) {
                getTopColumnsFromSelectQuery(select);
            } else if (select instanceof SQLUnionQuery){
                getTopColumnsFromUnionQuery(select);
            }
        }


        for (OutColumn column: outColumns){
            System.out.println(column.toString());
        }
    }

    //获取最外层的输出字段
    public static void getTopColumnsFromSelectQuery(SQLSelectQuery query) throws Exception {
        for (SQLSelectItem item: ((SQLSelectQueryBlock) query).getSelectList()){
            SQLExpr expr = item.getExpr();
            if (expr instanceof SQLPropertyExpr){
                SQLPropertyExpr s = (SQLPropertyExpr) expr;
                String columnOwner = s.getOwnerName();
                String columnName = s.getName();
                if (originalTables.containsKey(columnOwner)) {
                    //场景1:输出实体表所有字段
                    if (columnName.equals("*")){
                        for (String tableColumn: originalTables.get(columnOwner)){

                            outColumns.add(new OutColumn(
                                    String.format("%s.%s", columnOwner, tableColumn),tableColumn));
                        }
                    }
                    else {
                        //场景2:输出实体表单个字段
                        outColumns.add(
                                new OutColumn(s.toString(), item.getAlias() == null ? columnName : item.getAlias()));
                    }
                } else if (columnName.equals("*")) {
                    //场景3:输出子查询所有字段
                    getColumnsFromSubQuery(query, columnOwner, false);
                } else {
                    //场景4:输出单个字段
                    outColumns.add(new OutColumn(s.toString(), item.getAlias() == null ? columnName : item.getAlias()));
                }
            }
            else {
                String columnName = expr.toString();
                if (columnName.equals("*")){
                    //场景5:输出该查询所有字段
                    getColumnsFromQuery(query);
                }
                else {
                    //场景6:输出单个计算或者单个字段
                    outColumns.add(new OutColumn(columnName, item.getAlias() == null ? columnName : item.getAlias()));
                }
            }
        }
    }

    //获取最外层的输出字段
    public static void getTopColumnsFromUnionQuery(SQLSelectQuery query) throws Exception {
        //union查询只要获取某一边的字段即可
        SQLSelectQuery selectQuery = ((SQLUnionQuery) query).getLeft();
        if (selectQuery instanceof SQLSelectQueryBlock){
            getTopColumnsFromSelectQuery(selectQuery);
        } else {
            getTopColumnsFromUnionQuery(selectQuery);
        }
    }

    public static void getColumnsFromQuery(SQLObject o) throws Exception {
        if (o instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock select = (SQLSelectQueryBlock) o;
            SQLTableSource from = select.getFrom();
            if (from instanceof SQLExprTableSource) {
                String tableName = ((SQLExprTableSource) from).getTableName();
                for (SQLSelectItem item : select.getSelectList()) {
                    String columnExpr = item.getExpr().toString();
                    if (columnExpr.contains("*")) {
                        if (originalTables.containsKey(tableName)) {
                            for (String tableColumn : originalTables.get(tableName)) {
                                outColumns.add(new OutColumn(String.format("%s.%s", tableName, tableColumn), tableColumn));
                            }
                        } else {
                            throw new Exception(String.format("SQL代码中读取的表%s不在该组件前置节点中", tableName));
                        }
                    } else {
                        outColumns.add(new OutColumn(columnExpr, item.getAlias() == null ? columnExpr : item.getAlias()));
                    }
                }
            } else {
                getColumnsFromQuery(from);
            }
        } else if (o instanceof SQLSelect) {
            getColumnsFromQuery(((SQLSelect) o).getQuery());
        } else if (o instanceof SQLJoinTableSource) {
            getColumnsFromQuery(((SQLJoinTableSource) o).getLeft());
            getColumnsFromQuery(((SQLJoinTableSource) o).getRight());
        } else if (o instanceof SQLSubqueryTableSource) {
            getColumnsFromQuery(((SQLSubqueryTableSource) o).getSelect());
        } else if (o instanceof SQLExprTableSource) {
            String tableName = ((SQLExprTableSource) o).getTableName();
            if (originalTables.containsKey(tableName)) {
                for (String tableColumn : originalTables.get(tableName)) {
                    outColumns.add(new OutColumn(String.format("%s.%s", tableName, tableColumn), tableColumn));
                }
            } else {
                throw new Exception(String.format("SQL代码中读取的表%s不在该组件前置节点中", tableName));
            }
        }
    }

    public static void getColumnsFromSubQuery(SQLObject o, String subQueryName, boolean matchSubQueryName) throws Exception {
        if (o instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock select = (SQLSelectQueryBlock) o;
            SQLTableSource from = select.getFrom();
            //matchSubQueryName用于在该查询的父查询，进行子查询别名的判断
            if (from instanceof SQLExprTableSource && matchSubQueryName) {
                String tableName = ((SQLExprTableSource) from).getTableName();
                for (SQLSelectItem item : select.getSelectList()) {
                    String columnExpr = item.getExpr().toString();
                    if (columnExpr.contains("*")){
                        if (originalTables.containsKey(tableName)) {
                            for (String tableColumn : originalTables.get(tableName)) {
                                outColumns.add(new OutColumn(String.format("%s.%s", tableName, tableColumn), tableColumn));
                            }
                        } else {
                            throw new Exception(String.format("SQL代码中读取的表%s不在该组件前置节点中", tableName));
                        }
                    } else {
                        outColumns.add(
                                new OutColumn(String.format("%s.%s", subQueryName, columnExpr), item.getAlias() == null ? columnExpr : item.getAlias()));
                    }
                }
            } else {
                getColumnsFromSubQuery(from, subQueryName, false);
            }
        } else if (o instanceof SQLSelect) {
            getColumnsFromSubQuery(((SQLSelect) o).getQuery(), subQueryName, matchSubQueryName);
        } else if (o instanceof SQLJoinTableSource) {
            SQLTableSource left = ((SQLJoinTableSource) o).getLeft();
            getColumnsFromSubQuery(left, subQueryName, subQueryName.equals(left.getAlias()));
            SQLTableSource right = ((SQLJoinTableSource) o).getRight();
            getColumnsFromSubQuery(right, subQueryName, subQueryName.equals(right.getAlias()));
        } else if (o instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource subQuery = (SQLSubqueryTableSource) o;
            getColumnsFromSubQuery(subQuery.getSelect(), subQueryName, subQueryName.equals(subQuery.getAlias()));
        } else if (o instanceof SQLExprTableSource && matchSubQueryName) {
            String tableName = ((SQLExprTableSource) o).getTableName();
            if (originalTables.containsKey(tableName)) {
                for (String tableColumn : originalTables.get(tableName)) {
                    outColumns.add(new OutColumn(String.format("%s.%s", tableName, tableColumn), tableColumn));
                }
            } else {
                throw new Exception(String.format("SQL代码中读取的表%s不在该组件前置节点中", tableName));
            }
        }
    }

}