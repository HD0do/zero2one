package com.dida.practice.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Time;

/**
 * @Author：zhd
 * @Date: 2021/11/4 19:21
 * @Dscription:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class student {
    int id;
    String name;
    Time date;
}
