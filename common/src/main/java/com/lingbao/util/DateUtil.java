package com.lingbao.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author lingbao08
 * @DESCRIPTION
 * @create 2019-10-23 18:23
 **/

public class DateUtil {

    public static final DateTimeFormatter DTF_YMDHMS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//    .withZone(ZoneId.of("UTC+8"))

    public static LocalDateTime getDate(String dateStr) {
        return LocalDateTime.parse(dateStr, DTF_YMDHMS);
    }
}
