package com.lingbao.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lingbao08
 * @DESCRIPTION echats的饼状图实体
 * @create 2019-10-23 19:21
 **/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EchartsPie {

    private String name;

    private long value;
}
