package com.lingbao.flink.config

import java.sql.{Connection, DriverManager}

object MySQLConfig {

  def getConn(): Connection = {
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://127.0.0.1:3306/aqdata?useSSL=false&characterEncoding=utf8&serverTimezone=GMT"
    val username = "root"
    val pwd = "00000000"

    Class.forName(driver)
    DriverManager.getConnection(url, username, pwd)

  }

}
