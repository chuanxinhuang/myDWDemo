package com.xin.common.utils

import java.util.Properties

object JdbcUtils {


  val Properties = new Properties()

  Properties.setProperty("driver","com.mysql.jdbc.Driver")
  Properties.setProperty("user","root")
  Properties.setProperty("password","tiger")


  def  getProperties():Properties = {

    Properties
  }

}
