package com.xin.common.beans

case class LogBean(
                      val cookieid :String,
                      val account :String,
                      val imei :String,
                      val osName :String,
                      val osVer :String,
                      val resolution :String,
                      val androidId :String,
                      val manufacture :String,
                      val deviceId :String,
                      val appid :String,
                      val appVer :String,
                      val release_ch :String,
                      val promotion_ch :String,
                      val areacode :String,
                      val longtitude :Double,
                      val latitude :Double,
                      val carrier :String,
                      val netType :String,
                      val cid_sn :String,
                      val ip :String,
                      val sessionId :String,
                      val logType :String,
                      val commit_time:Long,
                      val eventMap: Map[String, String]
                    )


