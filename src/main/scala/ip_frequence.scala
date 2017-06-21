/**
  * Created by think on 2017/3/10.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.StatCounter
import java.text.SimpleDateFormat
import java.io._
object ip_frequence {
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

    val spark = SparkSession.builder().appName(args(0)).config("spark.some.config.option", "some_value").getOrCreate()


     var inputDF = spark.read.json("/crawldata")


    val interval = args(1).toInt;
    inputDF.createOrReplaceTempView("crawlTable")
   // spark.sql("select ip,uri,dfp,timestamp,concat(day(timestamp),\":\",hour(timestamp),\":\",(floor(minute(timestamp)/%d))*%d) as".format(interval/60,interval/60) +
   //   " index from crawlTable").write.mode("overwrite").parquet("midtable.parquet");
   spark.sql("select ip,uri,dfp,timestamp,concat(day(timestamp),\":\",(floor(hour(timestamp)/%d))*%d) as".format(interval/3600,interval/3600) +
    " index from crawlTable").write.mode("overwrite").parquet("midtable.parquet");

    var begin = 0;
    val time = Range(beginTime, endTime, interval);
    val midDF=spark.read.parquet("midtable.parquet");
    midDF.createOrReplaceTempView("midTable");
    //ip count

    spark.sql("select index,concat(ip,uri) as ip,count(1) as " +
      "frequence from midTable where ip is not null and uri is not null and (uri ==\"/pub/mortreginfo/detail\" or uri==\"/client/entsearch/list\" or uri==\"/midbranch/list.json\") group by ip,uri,index").write.mode("overwrite").parquet("frequence.parquet");
   // spark.sql("select index,dense_rank() over (partition by dfp order by uri) as ip,count(1) as " +
   //   "frequence from midTable where dfp is not null and uri is not null group by dfp,uri,index").write.mode("overwrite").parquet("frequence.parquet");
   // spark.sql("select index,concat(dfp,uri) as ip,count(1) as " +
   //   "frequence from midTable where dfp is not null and uri is not null group by dfp,uri,index").write.mode("overwrite").parquet("frequence.parquet");


    inputDF=spark.read.parquet("frequence.parquet");
    inputDF.createOrReplaceTempView("crawlTable");
    spark.sql("select a.ip as ip,a.std/a.mean as mutate from( select ip,mean(frequence) as " +
      "mean,stddev(frequence) as std from crawlTable group by ip) a ").write.mode("overwrite").parquet("frequence_std.parquet");
    inputDF=spark.read.parquet("frequence_std.parquet");
    inputDF.createOrReplaceTempView("stdTable");

    spark.sql("select ip,mean(frequence) as " +
      "frequence from crawlTable group by ip ").write.mode("overwrite").parquet("frequence_2.parquet");
    inputDF=spark.read.parquet("frequence_2.parquet");
    inputDF.createOrReplaceTempView("crawlTable");

    var writer = new PrintWriter(new File("/home/spark/zhjDir/" + args(2)));

    var maxStd = spark.sql("select max(frequence) as max from crawlTable where frequence!='NaN'").select("max").first;
    var minStd = spark.sql("select min(frequence) as min from crawlTable where frequence!='NaN'").select("min").first;
    //percentile
    val percStd = inputDF.stat.approxQuantile("frequence", Array(0.8), 0.001)

    if (maxStd(0) == null || minStd(0) == null) {

    }
    else {
      //val percStd=maxStd.getDouble(0)/50.0;
      val stdGap=(( percStd(0) - minStd.getDouble(0) ) / 100.0);
     // val stdGap=((maxStd.getDouble(0) - percStd(0)) / 100.0);


      val sql = "select 1 as id, a.index as index,count(1) as cnt from ( select floor((frequence-%f)/%f) as index from crawlTable".format(percStd(0),stdGap)+" where frequence!='NaN' and frequence < %f ) a group by a.index ".format(percStd(0));
     // val sql="select 1 as id,floor(frequence) as index,count(1) as cnt from crawlTable where frequence!='NaN' and frequence >= %f  group by floor(frequence)".format(percStd(0));
     //val sql="select 1 as id,floor(frequence) as index,count(1) as cnt from crawlTable where frequence!='NaN' group by floor(frequence)";
      spark.sql(sql).write.mode("overwrite").parquet("statis.parquet_global");
      val statisDF=spark.read.parquet("statis.parquet_global");
      statisDF.createOrReplaceTempView("statis_global");
      val result=spark.sql("select concat_ws(\",\",collect_set(concat(index,\":\",cnt))) as str from statis_global group by id").select("str").first.getString(0);




     // writer.write("beginTime:" + beginTime + " " + "endTime:" + endTime + " " + "interval:" + interval.toString() + " " + "maxStdev:"
    //    +  maxStd.getDouble(0).toString() + " " + "minStdev:" + percStd(0).toString() + " " + result + "\n");

      writer.write("beginTime:" + beginTime + " " + "endTime:" + endTime + " " + "interval:" + interval.toString() + " " + "maxStdev:"
        +  percStd(0).toString() + " " + "minStdev:" + minStd.getDouble(0).toString() + " " + result + "\n");

    };
    writer.close();


    writer = new PrintWriter(new File("/home/spark/zhjDir/mutate_" + args(2)));
    val nanNum=spark.sql("select count(*) as num from stdTable where mutate=='NaN'").select("num").first;
    maxStd = spark.sql("select max(mutate) as max from stdTable where mutate!='NaN'").select("max").first;
    minStd = spark.sql("select min(mutate) as min from stdTable where mutate!='NaN'").select("min").first;
    if (maxStd(0) == null || minStd(0) == null) {

    }
    else {
      val stdGap=((maxStd.getDouble(0) - minStd.getDouble(0)) / 100);



      val sql = "select 1 as id, a.index as index,count(1) as cnt from ( select floor((mutate-%f)/%f) as index from stdTable".format(minStd.getDouble(0),stdGap)+" where mutate!='NaN') a group by a.index ";
      spark.sql(sql).write.mode("overwrite").parquet("statis.parquet_global");
      val statisDF=spark.read.parquet("statis.parquet_global");
      statisDF.createOrReplaceTempView("statis_global");
      val result=spark.sql("select concat_ws(\",\",collect_set(concat(index,\":\",cnt))) as str from statis_global group by id").select("str").first.getString(0);




      writer.write("NaNNum:"+nanNum.getLong(0).toString()+"beginTime:" + beginTime + " " + "endTime:" + endTime + " " + "interval:" + interval.toString() + " " + "maxStdev:"
        + maxStd.getDouble(0).toString() + " " + "minStdev:" + minStd.getDouble(0).toString() + " " + result + "\n");

    };
    writer.close();
  }

}
