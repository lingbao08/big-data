package com.lingbao.entity;

import lombok.*;

import java.io.Serializable;

/**
 * @author lingbao08
 * @DESCRIPTION 和HBASE交互的类
 * @create 2019-10-23 18:35
 **/
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class GoingCount implements Serializable {

    private String day_desc;
    private long go_count;
}
