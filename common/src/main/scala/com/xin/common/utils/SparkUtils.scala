package com.xin.common.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def getSparkSession(name:String,master:String = "local[*]"):SparkSession = {
    SparkSession.builder().master(master).appName(name).getOrCreate()
  }

}
