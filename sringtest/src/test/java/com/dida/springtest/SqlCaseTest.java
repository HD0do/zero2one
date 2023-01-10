package com.dida.springtest;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @Author：zhd
 * @Date: 2023/1/9 11:52
 * @Dscription:
 */
@Slf4j
public class SqlCaseTest {


    @Test
    public void test() {
    String sql = "insert overwrite table kf_zt.account2 SELECT swjg_dm,swjgmc FROM (SELECT swjg_dm AS swjg_dm, swjgmc AS swjgmc FROM (SELECT swjg_dm,swjgmc FROM ( SELECT swjg_dm,swjgmc FROM yuangongs) table_350 UNION SELECT swjg_dm,swjgmc FROM (SELECT swjg_dm2,swjgmc2 FROM yuangong ) table_295) union_013) udf_882";

    List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql.toLowerCase(), JdbcConstants.HIVE);

    for (SQLStatement sqlStatement : sqlStatements) {
        SchemaStatVisitor schemaStatVisitor = SQLUtils.createSchemaStatVisitor(JdbcConstants.HIVE);
        sqlStatement.accept(schemaStatVisitor);
        Map<TableStat.Name, TableStat> tables = schemaStatVisitor.getTables();
        Collection<TableStat.Column> columns = schemaStatVisitor.getColumns();
        if (Objects.nonNull(tables)) {
            tables.forEach(((name, tableStat) -> {
                if (tableStat.getCreateCount() > 0 || tableStat.getInsertCount() > 0) {
                    log.info("to: table[{}]", name.getName().toLowerCase());
                    columns.stream().filter(column -> Objects.equals(column.getTable().toLowerCase(), name.getName().toLowerCase())).forEach(column -> {
                        log.info("to: table[{}] column[{}]", column.getTable().toLowerCase(), column.getName().toLowerCase());
                    });
                } else if (tableStat.getSelectCount() > 0) {
                    log.info("from: table[{}]", name.getName().toLowerCase());
                    columns.stream().filter(column -> Objects.equals(column.getTable().toLowerCase(), name.getName().toLowerCase())).forEach(column -> {
                        log.info("from: table[{}] column[{}]", column.getTable().toLowerCase(), column.getName().toLowerCase());
                    });
                }
            }));
        }
    }
}
}
