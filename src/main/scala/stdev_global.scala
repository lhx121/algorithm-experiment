/**
  * Created by think on 2017/3/10.
  */

import org.apache.spark.util.StatCounter
import java.text.SimpleDateFormat
import java.io._
object stdev_global {
  def stdIpTime_gloal(ip:String,ts_cs:String,interval:Int)={
    val timeSortedA= ts_cs.split(",");
    val date=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
    val timeSortedL=timeSortedA.map(a => date.parse(a).getTime).sorted;
    val statCG=new StatCounter();
    var testArray=List(1.0);
    for( time<-Range(1488124800,1488297600,interval) ) {
      val statC=new StatCounter();
      var begin=0;
      for(i<-Range(0,timeSortedA.size)){
        if(timeSortedL(i)/1000.0>=time && timeSortedL(i)/1000.0<=(time+interval) && begin==0) begin=1;
        if(begin==2 && timeSortedL(i)/1000.0>(time+interval)) begin=3;
        if(begin==2) statC.merge(timeSortedL(i)-timeSortedL(i-1));
        if(begin==1) begin=2;  } ;
      if(statC.mean>0.0) statCG.merge(statC.mean); };
    statCG.stdev;
  }

  def main(args:Array[String]) {
    // tempP interval storeFileName
    if (args.length < 3) {
      System.err.println("Usage:<tempParquetName> <intervalList in s> <storeFileName>")
      System.exit(1)
    }

    /*
        val spark = SparkSession.builder().appName(args(0)).config("spark.some.config.option", "some_value").getOrCreate()

        val inputDF=spark.read.json("crawldata/kafka_ip_uri.log")
        inputDF.createOrReplaceTempView("crawlTable")
        spark.sql("select ip,concat_ws(\",\",collect_set(timestamp)) as ts_cs from crawlTable group by ip").write.mode("overwrite").parquet("people.parquet");

    val ipAndTscsDF = spark.read.parquet("people.parquet");
    ipAndTscsDF.createOrReplaceTempView("ipAndTscs");
    spark.udf.register("stdIpTime", stdIpTime_gloal _)

    ipAndTscsDF.printSchema;

    val writer = new PrintWriter(new File("/home/hadoop/" + args(2)));
    val intervals = args(1).split(",").map(a => a.toInt);


    for (interval <- intervals) {
      spark.sql("select ip,stdIpTime(ip,ts_cs,%d) as ts_stdev from ipAndTscs where ip is not null".format(interval)).
        write.mode("overwrite")parquet("ip_ts_stdev_tempGlobal.parquet")

      val stdevDF = spark.read.parquet("ip_ts_stdev_tempGlobal.parquet")
      stdevDF.createOrReplaceTempView("crawlTable")

      val maxStd = spark.sql("select max(ts_stdev) as max from crawlTable where ts_stdev!='NaN'").select("max").first
      val minStd = spark.sql("select min(ts_stdev) as min from crawlTable where ts_stdev!='NaN'").select("min").first
      var arrayResult: List[Long] = List();
      for (i <- Range(0, 100)) {
        //将std为0的单独分出去 因为程序可能大概率以恒定间隔进行访问
        val cnt=spark.sql("select count(*) as cnt from crawlTable where ts_stdev!='NaN' and ts_stdev=0").select("cnt").first;
        arrayResult = arrayResult :+ cnt.getLong(0);
        val sql = ("select count(*) as cnt from crawlTable where ts_stdev!='NaN' and ts_stdev>%f*%d" +
          " and ts_stdev<=%f*(%d +1)").format((maxStd.getDouble(0) - minStd.getDouble(0)) / 100, i, (maxStd.getDouble(0) - minStd.getDouble(0)) / 100, i);
        val result = spark.sql(sql).select("cnt").first;
        arrayResult = arrayResult :+ result.getLong(0);
      }
      writer.write("interval:" + interval.toString() + " " + "maxStdev:"
        + maxStd.getDouble(0).toString() + " " + "minStdev:" + minStd.getDouble(0).toString() + " " + arrayResult.mkString(",") + "\n");
    }

    writer.close();
      */
  }
}



/*
/**
  * Created by think on 2017/3/10.
  */

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.util.StatCounter
import java.text.SimpleDateFormat
import java.io._
object scalatest {
  def stdIpTime_gloal(ip:String,ts_cs:String,interval:Int)={
    val timeSortedA= ts_cs.split(",");
    val date=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
    val timeSortedL=timeSortedA.map(a => date.parse(a).getTime).sorted;
    val statCG=new StatCounter();
    var testArray=List(1.0);
    for( time<-Range(1488124800,1488297600,interval) ) {
      val statC=new StatCounter();
      var begin=0;
      for(i<-Range(0,timeSortedA.size)){
        if(timeSortedL(i)/1000.0>=time && timeSortedL(i)/1000.0<=(time+interval) && begin==0) begin=1;
        if(begin==2 && timeSortedL(i)/1000.0>(time+interval)) begin=3;
        if(begin==2) statC.merge(timeSortedL(i)-timeSortedL(i-1));
        if(begin==1) begin=2;  } ;
      if(statC.mean>0.0) statCG.merge(statC.mean); };
    statCG.stdev;
  }
//local
  def stdIpTime(ip:String,ts_cs:String,interval:Int,beginT:Int,endT:Int)={
    val timeSortedA= ts_cs.split(",");
    val date=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
    val timeSortedL=timeSortedA.map(a => date.parse(a).getTime).sorted;
    val statCG=new StatCounter();var testArray=List(1.0);
    var result=0.0;
    val statC=new StatCounter();
    var begin=0;
    for(i<-Range(1,timeSortedA.size)){
      statC.merge(timeSortedL(i)-timeSortedL(i-1));
    } ;
    if(statC.mean>0.0) result=statC.stdev;else result=(-1.0);
    result;
}
  def main(args:Array[String]) {
    // tempP interval storeFileName
    if (args.length < 5) {
      System.err.println("Usage:<tempParquetName> <interval in s> <storeFileName> <beginTime> <endTime>")
      System.exit(1)
    }
    val beginTime = args(3).toInt;
    val endTime = args(4).toInt;

    //val sc:SparkContext;

    val sc = new SparkContext()
    val spark: SQLContext = new HiveContext(sc)


   // var inputDF = spark.read.json("crawldata")

    val interval = args(1).toInt;
    var writer = new PrintWriter(new File("/home/hadoop/zhjDir" + args(2)));
    var begin = 0;
    val time = Range(beginTime, endTime, interval);
    /*
    inputDF.registerTempTable("crawlTable")
    spark.sql("select ip,uri,dfp,timestamp,concat(day(timestamp),\":\",hour(timestamp),\":\",(minute(timestamp)/%d)*%d) as".format(interval/60,interval/60) +
      " index from crawlTable").write.mode("overwrite").parquet("midtable.parquet");
    val midDF=spark.read.parquet("midtable.parquet");
    midDF.registerTempTable("midTable");

    spark.sql("select index,ip,concat_ws(\",\",collect_set(timestamp)) as " +
      "ts_cs from midTable group by ip,index").write.mode("overwrite").parquet("index.parquet");
  */
    var inputDF=spark.read.parquet("index.parquet");

    time.par.foreach(x => {
      inputDF.registerTempTable("crawlTable");
      //spark.sql("select ip,uri,dfp,timestamp,concat(day(timestamp),\":\",hour(timestamp),\":\",(minute(timestamp)/5)*5) from crawlTable").show()
      spark.sql("select ip,ts_cs from crawlTable where inde" +
        "x=concat( day(from_unixtime(%d)),\":\",hour(from_unixtime(%d)),\":\",minute(from_unixtime(%d)) ) ".format(x,x,x)).
        write.mode("overwrite").parquet("people.parquet/"+x.toString);

      val ipAndTscsDF = spark.read.parquet("people.parquet/"+x.toString);
      ipAndTscsDF.registerTempTable("ipAndTscs"+x.toString);
      spark.udf.register("stdIpTime", stdIpTime _)
      //一个interval  间隔的ip ,stdev
      val sql = ("select ip,stdev as ts_stdev from(select ip, " +
        "stdIpTime(ip,ts_cs,%d,%d,%d) as stdev from ipAndTscs"+x.toString+" where ip is not null) a where a.stdev>0.0").format(interval, x, x + interval);



      spark.sql(sql).write.mode("overwrite").parquet(args(0)+"/"+x.toString);
      spark.sql(sql).write.mode("append").parquet(args(0)+"_global");

      val stdevDF = spark.read.parquet(args(0)+"/"+x.toString);
      stdevDF.registerTempTable("crawlTable"+x.toString);
      val maxStd = spark.sql("select max(ts_stdev) as max from crawlTable"+x.toString+" where ts_stdev!='NaN'").select("max").first;
      val minStd = spark.sql("select min(ts_stdev) as min from crawlTable"+x.toString+" where ts_stdev!='NaN'").select("min").first;
      if (maxStd(0) == null || minStd(0) == null) {

      }
      else {

        var arrayResult: List[Long] = List();
        for (i <- Range(0, 100)) {
          val sql = "select count(*) as cnt from crawlTable"+x.toString+" where ts_stdev!='NaN' and ts_stdev>=%f*%d and ts_stdev<%f*(%d +1)".
            format((maxStd.getDouble(0) - minStd.getDouble(0)) / 100, i, (maxStd.getDouble(0) - minStd.getDouble(0)) / 100, i);
          val result = spark.sql(sql).select("cnt").first;
          arrayResult = arrayResult :+ result.getLong(0);

        };
        writer.write("beginTime:" + x.toString() + " " + "endTime:" + (x + interval).toString() + " " + "interval:" + interval.toString() + " " + "maxStdev:"
          + maxStd.getDouble(0).toString() + " " + "minStdev:" + minStd.getDouble(0).toString() + " " + arrayResult.mkString(",") + "\n");
      };


    })
    writer.close();
    writer = new PrintWriter(new File("/home/hadoop/zhjDir" + "global_"+args(2)));

    var stdevDF = spark.read.parquet(args(0)+"_global");
    stdevDF.registerTempTable("crawlTable");
    spark.sql("select ip,stddev(ts_stdev) as stdev from crawlTable group by ip").write.mode("overwrite").parquet(args(0)+"_global_std") ;



    stdevDF = spark.read.parquet(args(0)+"_global_std");
    stdevDF.registerTempTable("crawlTable");
    val maxStd = spark.sql("select max(ts_stdev) as max from crawlTable where ts_stdev!='NaN'").select("max").first;
    val minStd = spark.sql("select min(ts_stdev) as min from crawlTable where ts_stdev!='NaN'").select("min").first;
    if (maxStd(0) == null || minStd(0) == null) {

    }
    else {

      var arrayResult: List[Long] = List();
      for (i <- Range(0, 100)) {
        val sql = "select count(*) as cnt from crawlTable where ts_stdev!='NaN' and ts_stdev>=%f*%d and ts_stdev<%f*(%d +1)".
          format((maxStd.getDouble(0) - minStd.getDouble(0)) / 100, i, (maxStd.getDouble(0) - minStd.getDouble(0)) / 100, i);
        val result = spark.sql(sql).select("cnt").first;
        arrayResult = arrayResult :+ result.getLong(0);

      };
      writer.write("beginTime:" + beginTime + " " + "endTime:" + endTime + " " + "interval:" + interval.toString() + " " + "maxStdev:"
        + maxStd.getDouble(0).toString() + " " + "minStdev:" + minStd.getDouble(0).toString() + " " + arrayResult.mkString(",") + "\n");
    };
    writer.close();

  }

}

*/
