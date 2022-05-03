package com.xiaoshouyi.item.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @Author：zhd
 * @Date: 2021/8/22 15:44
 * @Dscription: javabean中不需要的值，增加该注解，不在写入到clickhouse中
 */

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {

}
