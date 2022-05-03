package com.xiaoshouyi;

import java.util.ArrayList;
import java.util.List;

public class SQLUtils {
    public List<String> getColumns(String querysql){
        List<String> column = new ArrayList<String>();
        String tmp = querysql.substring(querysql.indexOf("select") + querysql.indexOf("from")).trim();
        if (tmp.indexOf("*") == 0){
            String cols[] = tmp.split(",");
            for (String c:cols){
                column.add(c);
            }
        }
        return column;
    }

    public String getTBname(String querysql){
        String tmp = querysql.substring(querysql.indexOf("from")+ 1).trim();
        int sx = tmp.indexOf(" ");
        if(sx == 0){
            return tmp;
        }else {
            return tmp.substring(0,sx);
        }
    }
}
