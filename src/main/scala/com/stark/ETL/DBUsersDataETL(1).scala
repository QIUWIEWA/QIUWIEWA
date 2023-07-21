package com.stark.ETL

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * 俄罗斯游戏网站Ongab数据库 db_users
  */
object DBUsersDataETL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DBUsersDataETL").setMaster("local[*]")
      .set("spark.network.timeout", "500")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.network.timeout", "500")
      .set("spark.shuffle.memoryFraction", "0.4")
      .set("spark.storage.memoryFraction", "0.3")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val es_index = "ongab_users"
    val map_new = Map("es.nodes" -> "10.100.100.91", ConfigurationOptions.ES_NET_HTTP_AUTH_USER -> "etl", ConfigurationOptions.ES_  NET_HTTP_AUTH_PASS -> "Stark@123")

    val fbAndVkMap = spark.read.option("header", "true").csv("E:\\pegging\\境外数据ALL\\20230224\\俄罗斯游戏网站Ongab数据库\\Ongab_BF\\data\\ongab_users_data.csv")
      .select("id", "vk_name", "fb_name", "vk_token", "fb_token").rdd.map(t => {
      val id = t.get(0).toString
      val vk_name =if (t.isNullAt(1) || t.get(1).toString.equals("NULL") || t.get(1).toString.equals("<blank>")) "" else t.get(1).toString
      val fb_name =if (t.isNullAt(2) || t.get(2).toString.equals("NULL") || t.get(2).toString.equals("<blank>")) "" else t.get(2).toString
      val vk_token =if (t.isNullAt(3) || t.get(3).toString.equals("NULL") || t.get(3).toString.equals("<blank>")) "" else t.get(3).toString
      val fb_token =if (t.isNullAt(4) || t.get(4).toString.equals("NULL") || t.get(4).toString.equals("<blank>")) "" else t.get(4).toString
      (id, (vk_name, fb_name, vk_token, fb_token))
    }).filter(t => !(t._2._1.isEmpty && t._2._2.isEmpty && t._2._3.isEmpty && t._2._4.isEmpty)).collectAsMap()


    val rowRDD = spark.read.option("header", "true").csv("E:\\pegging\\境外数据ALL\\20230224\\俄罗斯游戏网站Ongab数据库\\Ongab_BF\\data\\db_users.csv")
      .rdd.map(t => {
      val id = t.get(0).toString
      val facebook_id = if (t.isNullAt(2) || t.get(2).toString.equals("NULL")) "" else t.get(2).toString
      val yandex_id = if (t.isNullAt(3) || t.get(3).toString.equals("NULL")) "" else t.get(3).toString
      val mail_ru_id = if (t.isNullAt(4) || t.get(4).toString.equals("NULL")) "" else t.get(4).toString
      val google_id = if (t.isNullAt(5) || t.get(5).toString.equals("NULL")) "" else t.get(5).toString
      val twitter_id = if (t.isNullAt(6) || t.get(6).toString.equals("NULL")) "" else t.get(6).toString
      val vk_id = if (t.isNullAt(7) || t.get(7).toString.equals("NULL") || t.get(7).toString.equals("<blank>")) "" else t.get(7).toString
      val ip = if (t.isNullAt(9) || t.get(9).toString.equals("NULL")) "" else t.get(9).toString
      val icq = if (t.isNullAt(13) || t.get(13).toString.equals("NULL") || t.get(13).toString.equals("<blank>")) "" else t.get(13).toString
      val encrypt_field = if (t.isNullAt(15) || t.get(15).toString.equals("NULL")) "" else t.get(15).toString
      val website = if (t.isNullAt(16) || t.get(16).toString.equals("NULL") || t.get(16).toString.equals("<blank>")) "" else t.get(16).toString
      val source_nickname = if (t.isNullAt(22) || t.get(22).toString.equals("NULL") || t.get(22).toString.equals("<blank>")) "" else t.get(22).toString
      val nickname = source_nickname.toLowerCase
      val skype = if (t.isNullAt(25) || t.get(25).toString.equals("NULL") || t.get(25).toString.equals("<blank>")) "" else t.get(25).toString
      var source_mail = if (t.isNullAt(28) || t.get(28).toString.equals("NULL") || t.get(28).toString.equals("<blank>")) "" else t.get(28).toString
      if (!(source_mail.matches("[A-Za-z0-9]+[A-Za-z0-9_\\-\\.]{0,}@([A-Za-z0-9-]+\\.)+[A-Za-z]{1,20}$") || source_mail.isEmpty)) {
        source_mail = source_mail.toLowerCase
        source_mail = PeggingETLUtils.deleteParam(source_mail, Array('.', '-', '_'))
          .replace("@.", "@")
          .replaceAll("[,/+ !*\\[]", "")
        if (source_mail.contains("@mai")) {
          source_mail = source_mail.split("@").head + "@mail.ru"
        } else if (source_mail.contains("@inbox")) {
          source_mail = source_mail.split("@").head + "@inbox.ru"
        } else if (source_mail.contains("@yandex")) {
          source_mail = source_mail.split("@").head + "@yandex.ru"
        } else if (source_mail.contains("@bk")) {
          source_mail = source_mail.split("@").head + "@bk.ru"
        } else if (source_mail.contains("@gmail")) {
          source_mail = source_mail.split("@").head + "@gmail.com"
        } else if (source_mail.contains("@list")) {
          source_mail = source_mail.split("@").head + "@list.ru"
        }
        if (!(source_mail.matches("[A-Za-z0-9]+[A-Za-z0-9_\\-\\.]{0,}@([A-Za-z0-9-]+\\.)+[A-Za-z]{1,20}$") || source_mail.isEmpty)) {
          source_mail = ""
        }
      }
      val mail = source_mail.toLowerCase
      val phone = if (t.isNullAt(30) || t.get(30).toString.equals("NULL")) "" else t.get(30).toString
      val birthday = if (t.isNullAt(31) || t.get(31).toString.equals("NULL")) "" else t.get(31).toString.replace("-", "/")
      val gender = if (t.isNullAt(50) || t.get(50).toString.equals("NULL")) "" else t.get(50).toString match {
        case "m" => "M"
        case "w" => "F"
        case _ => ""
      }
      val game = if (t.isNullAt(51) || t.get(51).toString.equals("NULL") || t.get(51).toString.equals("<blank>")) "" else t.get(51).toString
      val wallet_yandex = if (t.isNullAt(52) || t.get(52).toString.equals("NULL") || t.get(52).toString.equals("<blank>")) "" else t.get(52).toString
      val about = if (t.isNullAt(53) || t.get(53).toString.equals("NULL") || t.get(53).toString.equals("<blank>")) "" else t.get(53).toString
      val hobby = if (t.isNullAt(55) || t.get(55).toString.equals("NULL")|| t.get(55).toString.equals("<blank>")) "" else t.get(55).toString
      val (vk_name, facebook_name, vk_token, facebook_token) = fbAndVkMap.getOrElse(id, ("", "", "", ""))
      val data_type = "Ongab"
      val country_code = "RU"
      Row(facebook_id,
        yandex_id,
        mail_ru_id,
        google_id,
        twitter_id,
        vk_id,
        ip,
        icq,
        encrypt_field,
        website,
        source_nickname,
        nickname,
        skype,
        source_mail,
        mail,
        phone,
        birthday,
        gender,
        game,
        wallet_yandex,
        about,
        hobby,
        vk_name,
        facebook_name,
        vk_token,
        facebook_token,
        data_type,
        country_code)
    })
    val dataSchema = new StructType()
      .add("facebook_id", StringType)
      .add("yandex_id", StringType)
      .add("mail_ru_id", StringType)
      .add("google_id", StringType)
      .add("twitter_id", StringType)
      .add("vk_id", StringType)
      .add("ip", StringType)
      .add("icq", StringType)
      .add("encrypt_field", StringType)
      .add("website", StringType)
      .add("source_nickname", StringType)
      .add("nickname", StringType)
      .add("skype", StringType)
      .add("source_mail", StringType)
      .add("mail", StringType)
      .add("phone", StringType)
      .add("birthday", StringType)
      .add("gender", StringType)
      .add("game", StringType)
      .add("wallet_yandex", StringType)
      .add("about", StringType)
      .add("hobby", StringType)
      .add("vk_name", StringType)
      .add("facebook_name", StringType)
      .add("vk_token", StringType)
      .add("facebook_token", StringType)
      .add("data_type", StringType)
      .add("country_code", StringType)

    val resultDF = spark.createDataFrame(rowRDD, dataSchema).distinct().coalesce(4)
    EsSparkSQL.saveToEs(resultDF, es_index, map_new)

    //    resultDF.show(100, 50)
    //    println(resultDF.count())
  }
}
