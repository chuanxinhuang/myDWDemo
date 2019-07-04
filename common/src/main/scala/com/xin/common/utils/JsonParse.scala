package com.xin.common.utils

import com.alibaba.fastjson.{JSON, JSONObject}
import com.xin.common.beans.LogBean
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}

object JsonParse {

  //2019-04-17 15:33:08,223 --> {"u":{"cookieid":"zgZi1la3e","account":"LS1jCW","email":"N9qPJE@tJNKb.com","phoneNbr":"24539600593","birthday":"1995-03-7","isRegistered":true,"isLogin":true,"addr":"tCKeExItu","gender":"F","phone":{"imei":"9YKD7DFEWCV1UF3S","osName":"ios","osVer":"8.2","resolution":"1024*768","androidId":"","manufacture":"apple","deviceId":"OGbtTYn4"},"app":{"appid":"com.51doit.mall","appVer":"2.2.8","release_ch":"金山手机助手","promotion_ch":"头条"},"loc":{"areacode":230128201,"longtitude":128.86860788140555,"latitude":46.02608920912226,"carrier":"中国电信","netType":"WIFI","cid_sn":"27HVaIEFjs5J","ip":"250.87.10.182"},"sessionId":"ofa29Y8JjS3R"},"logType":"startup","commit_time":1555486388223,"event":{}}

  /**
    * 通过自己解析json的方式，来将json日志转成beanRDD
    * @param dsLog
    * @return
    */
  def parseJson(dsLog:Dataset[String]) :RDD[LogBean]={

    dsLog.rdd.map(lines => {

      val jsonString: String = lines.split(" --> ")(1)

      var bean: LogBean = null
      try {
      val jSONObject: JSONObject = JSON.parseObject(jsonString)

      //用户信息
      val uobj: JSONObject = jSONObject.getJSONObject("u")
      val cookieid = uobj.getString("cookieid")
      val account = uobj.getString("account")

      // 取phone中的信息
      val phoneobj = uobj.getJSONObject("phone")
      val imei = phoneobj.getString("imei")
      val osName = phoneobj.getString("osName")
      val osVer = phoneobj.getString("osVer")
      val resolution = phoneobj.getString("resolution")
      val androidId = phoneobj.getString("androidId")
      val manufacture = phoneobj.getString("manufacture")
      val deviceId = phoneobj.getString("deviceId")

      // 取app信息
      val appobj = uobj.getJSONObject("app")
      val appid = appobj.getString("appid")
      val appVer = appobj.getString("appVer")
      val release_ch = appobj.getString("release_ch")
      val promotion_ch = appobj.getString("promotion_ch")


      // 取位置信息
      val locobj = uobj.getJSONObject("loc")
      val areacode = locobj.getString("areacode")
      val longtitude = locobj.getDouble("longtitude")
      val latitude = locobj.getDouble("latitude")
      val carrier = locobj.getString("carrier")
      val netType = locobj.getString("netType")
      val cid_sn = locobj.getString("cid_sn")
      val ip = locobj.getString("ip")

      // 去会话标识
      val sessionId = uobj.getString("sessionId")

      // 取事件类型
      val logType = jSONObject.getString("logType")

      // 事件提交时间
      val commit_time = jSONObject.getLong("commit_time")

      // event字段作为一个整体
      val event = jSONObject.getJSONObject("event")
      import scala.collection.JavaConversions._
      val eventMap: Map[String, String] = event.getInnerMap().map(tp => (tp._1, tp._2.toString)).toMap


      // 拼装结果返回
      bean = LogBean(
        cookieid,
        account,
        imei,
        osName,
        osVer,
        resolution,
        androidId,
        manufacture,
        deviceId,
        appid,
        appVer,
        release_ch,
        promotion_ch,
        areacode,
        longtitude,
        latitude,
        carrier,
        netType,
        cid_sn,
        ip,
        sessionId,
        logType,
        commit_time,
        eventMap
      )
    } catch {
      case e: Exception =>
    }
    bean
    })

  }

  /**
    * 用自定义schema的方式，让spark直接解析json数据为dataframe
    * @param dsLog
    * @param spark
    * @return
    */
  def parseJson(dsLog: Dataset[String],spark:SparkSession): DataFrame = {

    import spark.implicits._
    val jsonRDD: RDD[String] = dsLog.rdd.map(line=>line.split(" --> ")(1))
    val jsonDS = spark.createDataset(jsonRDD)


    val schema = new StructType()
      .add("u",new StructType()
        .add("cookieid",DataTypes.StringType)
        .add("account",DataTypes.StringType)
        .add("email",DataTypes.StringType)
        .add("phoneNbr",DataTypes.StringType)
        .add("birthday",DataTypes.StringType)
        .add("isRegistered",DataTypes.StringType)
        .add("isLogin",DataTypes.StringType)
        .add("addr",DataTypes.StringType)
        .add("gender",DataTypes.StringType)
        .add("sessionId",DataTypes.StringType)
        .add("phone",new StructType()
          .add("imei",DataTypes.StringType)
          .add("osName",DataTypes.StringType)
          .add("osVer",DataTypes.StringType)
          .add("resolution",DataTypes.StringType)
          .add("androidId",DataTypes.StringType)
          .add("manufacture",DataTypes.StringType)
          .add("deviceId",DataTypes.StringType)
        )
        .add("app",new StructType()
          .add("appid",DataTypes.StringType)
          .add("appVer",DataTypes.StringType)
          .add("release_ch",DataTypes.StringType)
          .add("promotion_ch",DataTypes.StringType)
        )
        .add("loc",new StructType()
          .add("areacode",DataTypes.StringType)
          .add("longtitude",DataTypes.DoubleType)
          .add("latitude",DataTypes.DoubleType)
          .add("carrier",DataTypes.StringType)
          .add("netType",DataTypes.StringType)
          .add("cid_sn",DataTypes.StringType)
          .add("ip",DataTypes.StringType)
        )
      )
      .add("logType",DataTypes.StringType)
      .add("commit_time",DataTypes.LongType)
      .add("event",DataTypes.createMapType(DataTypes.StringType,DataTypes.StringType))

    spark.read.schema(schema).json(jsonDS)

  }

}
