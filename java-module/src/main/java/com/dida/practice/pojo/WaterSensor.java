package com.dida.practice.pojo;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author：zhd
 * @Date: 2023/3/6 11:57
 * @Dscription:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {

    @ApiModelProperty("编号")
    private String id;
    @ApiModelProperty("时间戳")
    private Long ts;
    @ApiModelProperty("水位线")
    private Integer vc;

}
