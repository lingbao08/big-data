package com.lingbao.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * @author lingbao08
 * @DESCRIPTION 这个类是从source进来后，将source的数据在spark中进行处理的类
 * @create 2019-10-23 18:35
 **/

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Going implements Serializable {

    private LocalDateTime date;
    private String ip;
    private String url;
    private String httpRef;


}
