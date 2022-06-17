package com.dida.realtime.pojo;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author：zhd
 * @Date: 2021/12/24 13:35
 * @Dscription: 用户行为类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserLog {
    Long  timestamp;
    Long  pv =0L;
    JSONObject event;
}
