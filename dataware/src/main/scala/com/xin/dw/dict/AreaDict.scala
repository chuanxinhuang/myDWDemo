package com.xin.dw.dict

import ch.hsr.geohash.GeoHash
import com.xin.common.utils.{JdbcUtils, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object AreaDict {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

  val spark: SparkSession = SparkUtils.getSparkSession("ll")

    import spark.implicits._

    val df: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/dicts?charactorEncoding=utf-8","areas",JdbcUtils.getProperties())

    val res: DataFrame = df.where("lng is not null and lat is not null")
      .map({

        case Row(id: String, province: String, city: String, district: String, county: String, lng: Double, lat: Double) =>

          val geo: String = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 6)

          (geo, province, city, district, county)

      }).toDF("geo", "province", "city", "district", "county")


//    res.write.parquet("C:\\Users\\Huang\\Desktop\\ml-1m\\out1")
//    res.write.json("C:\\Users\\Huang\\Desktop\\ml-1m\\out2")
//    res.write.csv("C:\\Users\\Huang\\Desktop\\ml-1m\\out3")

    res.write.jdbc("jdbc:mysql://localhost:3306/dicts?charactorEncoding=utf-8","busnessareas",JdbcUtils.getProperties())
    //

  spark.close()



    //C:\Users\Huang\Desktop\ml-1m\out

  }

}
