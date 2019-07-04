package com.xin.dw.ETL

import com.xin.common.beans.LogBean
import com.xin.common.utils.{JsonParse, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object PreProcess {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkUtils.getSparkSession("preprocess")
    import scala.collection.JavaConversions._

    val dsLog: Dataset[String] = spark.read.textFile("C:\\Users\\Huang\\Desktop\\ml-1m\\mall.access.log")

//  ds.show(10,false)

     // val dsArea: DataFrame = spark.read.parquet("C:\\Users\\Huang\\Desktop\\ml-1m\\parquetout")
    //行政位置圈
      val dsArea: DataFrame = spark.read.parquet("C:\\Users\\Huang\\Desktop\\ml-1m\\data\\area\\areaout")
//      val dsArea: DataFrame = spark.read.parquet("C:\\Users\\Huang\\Desktop\\ml-1m\\data\\area\\bizout")
//      val dsArea: DataFrame = spark.read.parquet("C:\\Users\\Huang\\Desktop\\ml-1m\\data\\output-pre")
    val areaMap: Map[String, (String, String, String, String)] = dsArea.collectAsList().map({ case Row(geo: String, p: String, c: String, d: String, t: String) => (geo, (p, c, d, t)) }).toMap
    val areaBC: Broadcast[Map[String, (String, String, String, String)]] = spark.sparkContext.broadcast(areaMap)


    //商圈位置
    val bizArea: DataFrame = spark.read.parquet("C:\\Users\\Huang\\Desktop\\ml-1m\\data\\area\\bizout")

    bizArea.collectAsList().map({
      case Row(geo: String, p: String, c: String, d: String, biz: String) => (geo, (p, c, d, biz))
    }).groupBy(_._1)
      .mapValues(iter=>iter.map(_._2).toList)
      .map(x=>x)

    val bizBC: Broadcast[DataFrame] = spark.sparkContext.broadcast(bizArea)

    // 逐调用json解析工具方法，逐行处理流量日志：解析json为LogBean
    val beanRdd: RDD[LogBean] = JsonParse.parseJson(dsLog)

    val washed: RDD[LogBean] = beanRdd.filter(_ != null).filter(bean => {

      // 判断数据是否符合规则
      // account && sessionid &&  imei && deviceId && androidId 不能全为空
      StringUtils.isNotBlank(bean.account) ||
        StringUtils.isNotBlank(bean.sessionId) ||
        StringUtils.isNotBlank(bean.imei) ||
        StringUtils.isNotBlank(bean.deviceId) ||
        StringUtils.isNotBlank(bean.androidId)

    })


//    查看阶段性成果
//        washed.take(10).foreach(println)
//        sys.exit()







    spark.close()


  }
}
