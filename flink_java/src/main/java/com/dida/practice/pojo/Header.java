package com.dida.practice.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Decimal;

/**
 * @Author：zhd
 * @Date: 2021/12/15 19:51
 * @Dscription:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Header {
    int id;
    String tenant_id;
    String shop;
    Decimal amt;
}
