/**
  * Created by think on 2017/3/10.
  */

import org.apache.spark.sql.SparkSession

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

  def stdIpTime(ts_cs:String)={
    val timeSortedA= ts_cs.split(",");
    val date=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
    val timeSortedL=timeSortedA.map(a => date.parse(a).getTime).sorted;
    val statCG=new StatCounter();var testArray=List(1.0);
    var result=(-1.0,-1.0);
    val statC=new StatCounter();
    var begin=0;
    for(i<-Range(1,timeSortedA.size)){//TODO Filter by interval /(diffTime) >X

      statC.merge(timeSortedL(i)-timeSortedL(i-1));
    } ;
    if(statC.mean>0.0) result=(statC.mean,statC.stdev);else result=(-1.0,-1.0);
    if(statC.mean<0.0) result=(-2.0,-2.0);
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

    val spark = SparkSession.builder().appName(args(0)).config("spark.some.config.option", "some_value").getOrCreate()
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
   // val sc = new SparkContext()
    //val spark: SQLContext = new HiveContext(sc)

    var inputDF = spark.read.json("/crawldata")

    val interval = args(1).toInt;

    var begin = 0;
    val time = Range(beginTime, endTime, interval);
    spark.udf.register("stdIpTime", stdIpTime _)
    inputDF.createOrReplaceTempView("crawlTable")
    //spark.sql("select ip,uri,dfp,timestamp,concat(day(timestamp),\":\",hour(timestamp) ,\":\",(floor(minute(timestamp)/%d)*%d)  ) as".format(interval/60,interval/60) +
   //   " index from crawlTable").write.mode("overwrite").parquet("midtable.parquet");
    spark.sql("select ip,uri,dfp,timestamp,concat(day(timestamp),\":\",(floor(hour(timestamp)/%d)*%d)  ) as".format(interval/3600,interval/3600) +
      " index from crawlTable").write.mode("overwrite").parquet("midtable.parquet");
    val midDF=spark.read.parquet("midtable.parquet");
    midDF.createOrReplaceTempView("midTable");

    //change key from here
    spark.sql("select index,concat(ip,uri) as ip,concat_ws(\",\",collect_set(timestamp)) as " +
      "ts_cs,count(1) as frequence from midTable where ip is not null and uri is not null group by ip,uri,index").write.mode("overwrite").parquet("index.parquet");
    //TODO filter NUM(group by ip) <X
    inputDF=spark.read.parquet("index.parquet");
    inputDF.createOrReplaceTempView("fileterTable")
    val perc = inputDF.stat.approxQuantile("frequence", Array(0.8), 0.001)
    spark.sql("select * from fileterTable where frequence <=%f".format(perc(0)) ).write.mode("overwrite").parquet("filter.parquet")
    inputDF=spark.read.parquet("filter.parquet");
    inputDF.createOrReplaceTempView("crawlTable");
    //这里其实过滤掉了访问是1次的，时间间隔越大，访问1次的越多？
    spark.sql("select index,ip,stdev as ts_stdev,mean from(select index,ip, " +
        "stdIpTime(ts_cs)[\"_1\"] as mean ,stdIpTime(ts_cs)[\"_2\"] as stdev from crawlTable ) a where a.stdev>(-1.0)").write.mode("overwrite").parquet(args(0));


  //  spark.sql("select * from(select index,ip, " +
   //   "stdIpTime(ts_cs)[\"_1\"] as mean ,stdIpTime(ts_cs)[\"_2\"] as stdev from crawlTable ) a where a.stdev=(-2.0)").write.mode("overwrite").parquet(args(0));

    inputDF=spark.read.parquet(args(0));
    inputDF.createOrReplaceTempView("crawlTable");
    //global-------------

    var writer = new PrintWriter(new File("/home/spark/zhjDir/" + "global_"+args(2)));

    spark.sql("select ip,stddev(mean) as ts_stdev from crawlTable group by ip").write.mode("overwrite").parquet(args(0)+"_global_std") ;
    val globalDF = spark.read.parquet(args(0)+"_global_std");
    globalDF.createOrReplaceTempView("globalTable");
    val maxStd = spark.sql("select max(ts_stdev) as max from globalTable where ts_stdev!='NaN'").select("max").first;
    val minStd = spark.sql("select min(ts_stdev) as min from globalTable where ts_stdev!='NaN'").select("min").first;

    if (maxStd(0) == null || minStd(0) == null) {

    }
    else {


        val stdGap=((maxStd.getDouble(0) - minStd.getDouble(0)) / 100);



        val sql = "select 1 as id, a.index as index,count(1) as cnt from ( select floor((ts_stdev-%f)/%f) as index from globalTable".format(minStd.getDouble(0),stdGap)+" where ts_stdev!='NaN') a group by a.index ";
        spark.sql(sql).write.mode("overwrite").parquet("statis.parquet_global");
        val statisDF=spark.read.parquet("statis.parquet_global");
        statisDF.createOrReplaceTempView("statis_global");
        val result=spark.sql("select concat_ws(\",\",collect_set(concat(index,\":\",cnt))) as str from statis_global group by id").select("str").first.getString(0);




      writer.write("beginTime:" + beginTime + " " + "endTime:" + endTime + " " + "interval:" + interval.toString() + " " + "maxStdev:"
        + maxStd.getDouble(0).toString() + " " + "minStdev:" + minStd.getDouble(0).toString() + " " + result + "\n");
    };
    writer.close();
    //gloabal -----------------

    writer = new PrintWriter(new File("/home/spark/zhjDir/" + args(2)));

    time.foreach(x => {

     // spark.sql("select ip,ts_stdev from crawlTable where inde" +
    //        "x=concat( day(from_unixtime(%d)),\":\",hour(from_unixtime(%d)),\":\",minute(from_unixtime(%d)) ) ".format(x,x,x)).
     //      write.mode("overwrite").parquet("people.parquet/"+x.toString);
      spark.sql("select ip,ts_stdev from crawlTable where inde" +
        "x=concat( day(from_unixtime(%d)),\":\",hour(from_unixtime(%d)) ) ".format(x,x)).
        write.mode("overwrite").parquet("people.parquet/"+x.toString);

      val ipAndTscsDF = spark.read.parquet("people.parquet/"+x.toString);
      ipAndTscsDF.createOrReplaceTempView("ipAndTscs"+x.toString);

      val maxStd = spark.sql("select max(ts_stdev) as max from ipAndTscs"+x.toString+" where ts_stdev!='NaN'").select("max").first;
      val minStd = spark.sql("select min(ts_stdev) as min from ipAndTscs"+x.toString+" where ts_stdev!='NaN'").select("min").first;
      if (maxStd(0) == null || minStd(0) == null) {

      }
      else {
        val stdGap=((maxStd.getDouble(0) - minStd.getDouble(0)) / 100);



        val sql = "select 1 as id, a.index as index,count(1) as cnt from ( select floor((ts_stdev-%f)/%f) as index from ipAndTscs".format(minStd.getDouble(0),stdGap)+x.toString+" where ts_stdev!='NaN') a group by a.index ";
         spark.sql(sql).write.mode("overwrite").parquet("statis.parquet/"+x.toString);
        val statisDF=spark.read.parquet("statis.parquet/"+x.toString);
        statisDF.createOrReplaceTempView("statis"+x.toString);
        val result=spark.sql("select concat_ws(\",\",collect_set(concat(index,\":\",cnt))) as str from statis"+x.toString+ " group by id").select("str").first.getString(0);


        print(result)
        writer.write("beginTime:" + x.toString() + " " + "endTime:" + (x + interval).toString() + " " + "interval:" + interval.toString() + " " + "maxStdev:"
          + maxStd.getDouble(0).toString() + " " + "minStdev:" + minStd.getDouble(0).toString() + " " + result + "\n");
      };


    })
    writer.close();

  }

}
