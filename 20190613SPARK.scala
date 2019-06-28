package com.spark.c4_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

object s1_rddIntro {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    ///////////////////////////  데이터 파일 로딩  ////////////////////////////////////
    // 파일명 설정 및 파일 읽기
    var selloutFile = "KOPO_PRODUCT_VOLUME.csv"

    // 절대경로 입력
    var selloutData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("C:/spark/bin/data/dataset/"+selloutFile)

    // 데이터 확인
    println(selloutData.show)

    // 컬럼별 인덱스 생성
    var rddColumns = selloutData.columns

    // 컬럼별 인덱스 조회
    // 코드의 가독성을 위해서 컬럼에 있는 모든 정보를 인덱스로 만듬.
    var regionidNo = rddColumns.indexOf("REGIONID")
    var productNo = rddColumns.indexOf("PRODUCTGROUP")
    var yearweekNo = rddColumns.indexOf("YEARWEEK")
    var volumeNo = rddColumns.indexOf("VOLUME")

    // 데이터프레임 -> RDD 변환 (1)
    var selloutRdd = selloutData.rdd

    // 데이터 정제
    // regionid, productgroup, yearweek, volume
    var filteredRdd = selloutRdd.filter(x=>{
      // 디버깅 코드
      // var x = selloutRdd.first
      // (많은 데이터중에서 랜덤으로 하나만 x에 담게됨)
      // 하나씩 디버깅 가능함

      // 한줄씩 True or False 체크
      var checkValid = true

      // 한행 내 특정 컬럼 추출
      var yearweek = x.getString(yearweekNo)
      var regionName = x.getString(regionidNo)

      // 조건 수행
      // yearweek의 길이가 6을 넘으면 이상한 데이터므로 걸러야함.
      if((yearweek.length > 6) ||
        (yearweek.substring(4,6).toInt > 52) ||
        (regionName != "A01")) {
        checkValid = false
      }
      checkValid
    })

    // 데이터 정제
    // regionid, productgroup, yearweek, volume
    // 구글 docx 문제 6번
//        var filteredRdd = selloutRdd.filter(x=>{
//          var checkValid = false
//
//          var yearInfo = x.getString(yearweekNo).substring(0, 4).toInt
//          var weekInfo = x.getString(yearweekNo).substring(4, 6).toInt
//          var productSet = x.getString(productNo)
//
//          if((weekInfo < 50) &&
//            (yearInfo >= 2015) &&
//            (productSet.contains(x.getString(productNo)))) {
//            checkValid = true
//          }
//          checkValid
//        })

    var MAXVOLUME = 700000
    var upperLimitSellout = filteredRdd.map(row=> {
      // 디버깅 코드
      // var row = filterRddQuiz.first

      //Logic: if volume is upper than maxvolume change to maxvolume
      var org_volume = row.getString(volumeNo).toDouble
      var new_volume = 0.0d
      if(org_volume >= MAXVOLUME) {
        new_volume = 700000
      }else {
        new_volume = org_volume
      }
      // Define output columns
      Row(row.getString(regionidNo),
        row.getString(productNo),
        row.getString(yearweekNo),
        new_volume)
    })

//    var MINVOLUME = 150000
//        var lowerLimitSellout = filteredRdd.map(row=> {
//          // 디버깅 코드
//          // var row = filterRddQuiz.first
//
//          //Logic: if volume is upper than maxvolume change to maxvolume
//          var org_volume = row.getString(volumeNo).toDouble
//          var new_volume = 0.0d
//          if(org_volume <= MINVOLUME) {
//        new_volume = 150000
//      }else {
//        new_volume = org_volume
//      }
//      // Define output columns
//      Row(row.getString(regionidNo),
//        new_volume,
//        row.getString(productNo),
//        row.getString(yearweekNo),
//        ((new_volume * 120).round)/100.toDouble)
//    })
//
//    var nextRdd = upperLimitSellout.map(x=>{
//      var org_volume = x.getDouble(volumeNo)
//    })

    //upperLimitSellout.collect.foreach(println)
    //lowerLimitSellout.collect.foreach(println)
    //nextRdd.collect.foreach(println)
    //groupRdd.collect.foreach(println)
    //groupMap.collect.foreach(println)

    // regionid, productgroup, yearweek, volume
    // 2. 지역, 상품별 평균 거래량 산출
    var groupRdd = upperLimitSellout.groupBy(x=>{
      (x.getString(regionidNo),
      x.getString(productNo))}).map(x=>{
      // 그룹별 분산처리가 수행됨
      var key = x._1
      var data = x._2
      var volumeSum = data.map(x=> {x.getDouble(volumeNo)}).sum
      var size = data.size
      var average = 0.0d
      if(size !=0) {
        average = volumeSum / size
      } else {
        average = 0.0d
      }
      (key, (size,average))
    })

    // GROUP MAP 함수 생성
    var groupMap = groupRdd.collectAsMap()

    //Group MAP 활용
    var targetregion = "A01"
    var targetProduct = "ST0001"
    if(groupMap.contains(targetregion, targetProduct)){
      var testValue = groupMap(targetregion, targetProduct)
    }

    //RDD -> 데이터 프레임
    // 으로 변환
    var resultRowDf = spark.createDataFrame(filteredRdd,
      StructType(Seq(
        StructField("REGIONID", StringType),
        StructField("PRODUCT", StringType),
        StructField("YEARWEEK", StringType),
        StructField("VOLUME", StringType)
      )))
  }
}
