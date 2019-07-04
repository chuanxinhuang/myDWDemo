package com.xin.dw.dict

import ch.hsr.geohash.GeoHash
import com.xin.common.utils.SparkUtils
import org.apache.spark.sql.{Dataset, SparkSession}

object BizDict {
  def main(args: Array[String]): Unit = {



  val spark: SparkSession = SparkUtils.getSparkSession("BizDict")
    import spark.implicits._

    val ds: Dataset[String] = spark.read.textFile("C:\\Users\\Huang\\Desktop\\ml-1m\\商圈数据.sql")
    val res = ds.map(lines=>{
      val fileds: Array[String] = lines.split("'")
      //"geo", "province", "city", "district", "county"
      val province = fileds(3)
      val city = fileds(5)
      val district = fileds(7)
      val biz = fileds(9)
      val geo = GeoHash.geoHashStringWithCharacterPrecision(fileds(13).toDouble,fileds(11).toDouble,6)
      (geo,province,city,district,biz)

    }).toDF("geo","province","city","district","biz")


    res.write.parquet("C:\\Users\\Huang\\Desktop\\ml-1m\\parquetout")

    spark.close()
  }

}
