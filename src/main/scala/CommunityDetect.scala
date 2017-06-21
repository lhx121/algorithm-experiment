/**
  * Created by think on 2017/4/25.
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.io._
import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.graphstream.graph.{Graph => GraphStream}
import org.graphstream.graph.implementations._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/** Label Propagation algorithm. */
object LabelPropagation extends Serializable{
  /**
    * Run static Label Propagation for detecting communities in networks.
    *
    * Each node in the network is initially assigned to its own community. At every superstep, nodes
    * send their community affiliation to all neighbors and update their state to the mode community
    * affiliation of incoming messages.
    *
    * LPA is a standard community detection algorithm for graphs. It is very inexpensive
    * computationally, although (1) convergence is not guaranteed and (2) one can end up with
    * trivial solutions (all nodes are identified into a single community).
    *
    * @tparam ED the edge attribute type (not used in the computation)
    * @param graph the graph for which to compute the community affiliation
    * @param maxSteps the number of supersteps of LPA to be performed. Because this is a static
    * implementation, the algorithm will run for exactly this many supersteps.
    * @return a graph with vertex attributes containing the label of community affiliation
    */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

    val lpaGraph = graph.mapVertices { case (vid,attr) => attr.asInstanceOf[Long] }  //label only
    //val lpaGraph = graph.mapVertices { case (vid,attr) => (attr.asInstanceOf[Long],1.0) }  //label and weight
    //val lpaGraph = graph

    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[Long, Double])] = {
      Iterator((e.srcId, Map(e.dstAttr -> 1*e.attr.asInstanceOf[Double])), (e.dstId, Map(e.srcAttr -> 1*e.attr.asInstanceOf[Double])))
    }

    def mergeMessage(count1: Map[Long, Double], count2: Map[Long, Double])
    : Map[Long, Double] = {
      (count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0.0)
        val count2Val = count2.getOrElse(i, 0.0)
        i -> (count1Val + count2Val)
      }.toMap
    }

    def vertexProgram(vid: VertexId, attr: Long, message: Map[Long, Double]): Long = {
      if (message.isEmpty) attr else message.maxBy(_._2)._1 //?
    }
    //label edgeWeightSum
    val initialMessage = Map[Long, Double]()
    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }
}


/** BM Label Propagation algorithm. */
object BMLabelPropagation extends Serializable{
  /**
    * @tparam ED the edge attribute type (not used in the computation)
    * @param graph the graph for which to compute the community affiliation
    * @param maxSteps the number of supersteps of LPA to be performed. Because this is a static
    * implementation, the algorithm will run for exactly this many supersteps.
    * @return a graph with vertex attributes containing the label of community affiliation
    */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[Map[Long,Double], ED] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

    //val lpaGraph = graph.mapVertices { case (vid,attr) => attr.asInstanceOf[Long] }  //label only
    val lpaGraph = graph.mapVertices { case (vid,attr) =>  Map(attr.asInstanceOf[Long]->1.0) }  //label and weight
    //val lpaGraph = graph

    def sendMessage(e: EdgeTriplet[Map[VertexId,Double], ED]): Iterator[(VertexId, Map[Long, Double])] = {
      //Iterator((e.srcId, Map(e.dstAttr -> 1*e.attr.asInstanceOf[Double])), (e.dstId, Map(e.srcAttr -> 1*e.attr.asInstanceOf[Double])))

      Iterator((e.srcId, e.dstAttr.keySet.map{i=> i-> (e.dstAttr.getOrElse(i,0.0)*e.attr.asInstanceOf[Double])}.toMap ), ( e.dstId,e.srcAttr.keySet.map{i=> i-> (e.srcAttr.getOrElse(i,0.0)*e.attr.asInstanceOf[Double])}.toMap) )
    }

    //Label 以及label的概率值
    def mergeMessage(count1: Map[Long, Double], count2: Map[Long, Double])
    : Map[Long, Double] = {

      val label_prob_map=(count1.keySet ++ count2.keySet).map { i =>
        val count1Val = count1.getOrElse(i, 0.0)
        val count2Val = count2.getOrElse(i, 0.0)
        i -> (count1Val + count2Val)
      }.toMap
      //缩放
      val max_prob=label_prob_map.maxBy(_._2)._2

      var sum_prob=0.0;
      val label_prob_map_scale=label_prob_map.keySet.map{
        i=> val new_prob=label_prob_map.getOrElse(i,0.0)/max_prob; sum_prob+=new_prob; i->new_prob
      }.toMap
      //归一化
      label_prob_map_scale.keySet.map{
        i=> val new_prob=label_prob_map_scale.getOrElse(i,0.0)/sum_prob; i->new_prob
      }.toMap

    }
    //TODO  filter：过滤掉低于某prob值的id,prob对
    def vertexProgram(vid: VertexId, attr: Map[Long,Double], message: Map[Long, Double]):Map[Long,Double] = {
      if (message.isEmpty || attr.getOrElse(1L,0.0) == 1.0) attr else message;
    }
    //label edgeWeightSum
    val initialMessage = Map[Long, Double]()
    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage
    )}
}

object CommunityDetect {
  //TODO 增加参数attrWeight使属性权重可设置

  def simCompute(strAttrL:String,strAttrR:String) = {
    var score=0;
    val attrSize=strAttrL.split(",").length;
    val listAttrL=strAttrL.split(",");
    val listAttrR=strAttrR.split(",");
    if (attrSize==strAttrR.split(",").length) {
      for (i <- Range(0, attrSize)) {
        if (listAttrL(i) == listAttrR(i) && listAttrR(i) != "") score = score + 1;
      }
    }
    val result=(score*1.0)/attrSize;
    result;
  }

  def main(args:Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage:<payOrderTableName> <listAttrFocus> <listAttrJoinKey> <listAttrCompSim> <cust_key> <blackOrderTableName> <black_key>")
      System.exit(1)
    }
    val payOrderName=args(0).toString;
    val focusAttr=args(1).toString;
    require(focusAttr.split(",").length > 1, s"Maximum of steps must be greater than 0, but got ${focusAttr.split(",").length}")
    //focustAttr应大于1
    val joinKey=args(2).toString;
    //joinkey长度应不超过1
    require(joinKey.split(",").length == 1, s"Maximum of steps must be greater than 0, but got ${joinKey.split(",").length}")
    val simAttr=args(3).toString
    val cust_id=args(4).toString

    val blackOrderName=args(5).toString
    val black_key=args(6).toString

    require(cust_id.split(",").length == 1, s"Maximum of steps must be greater than 0, but got ${joinKey.split(",").length}")
    val spark = SparkSession
      .builder()
      .appName("SparkSession")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    //convert mysql hive data to file for graph build

    val joinKeyCon=joinKey.split(",").map(x=>x+"!=\"\"").mkString(",")
    val focusDF=spark.sql("select %s from %s where %s group by %s".format(focusAttr,payOrderName,joinKeyCon,focusAttr))
    //focusDF.write.mode("overwrite").parquet("focusInPayOrder")
    focusDF.createOrReplaceTempView("focusTable")
    val simAttrL=simAttr.split(",").map(x=>"a."+x).mkString(",")
    val simAttrR=simAttr.split(",").map(x=>"b."+x).mkString(",")
    val joinKeyL=joinKey.split(",").map(x=>"a."+x).mkString(",")
    val joinKeyR=joinKey.split(",").map(x=>"b."+x)mkString(",")
    val whereCon=joinKey.split(",").map(x=>"a."+x+"!="+"b."+x).mkString(",")
    spark.udf.register("simCompute", simCompute _)
    //数据  usr_id为空的都是通过web交易的数据，其卡号之类信息也是不全的
    //从payOrder表中提取 非空joinkey
    val notnull=joinKey.split(",").map(x=>x+"!=\"\"").mkString(",")
    val datdDF=spark.sql("select * from %s where %s".format(payOrderName,notnull))
    datdDF.createOrReplaceTempView("filtedPayOrder")
    // 例：将所有usr_id不同，但其身份证号、终端标识、银行卡号、ip，手机号等类型的连接，并以相似度作为边权重
    //这里需要o(n^2)以上的时间
    //TODO 优化?
    val simTest=spark.sql("select %s as l1,%s as l2,simCompute(concat_ws(',',%s),concat_ws(',',%s)) as weight from filtedPayOrder a join focusTable b on %s".format(joinKeyL,joinKeyR,simAttrL,simAttrR,whereCon))
    //simTest.write.mode("overwrite").parquet("weightParquet")
    simTest.createOrReplaceTempView("weightTable")
    //val edgeDF=spark.sql("select * from weightTable where weight>0 group by l1,l2,weight")
    val edgeDF=spark.sql("select l1,l2,max(weight) as weight from weightTable where weight>0 group by l1,l2")
    val relationShips:RDD[Edge[Double]]=edgeDF.rdd.map{x:org.apache.spark.sql.Row=>Edge(x.getAs[String](0).toLong,x.getAs[String](1).toLong,x.getAs[Double](2))}
    var unionRDD=relationShips
    //例 ：将usr_id与mer_no连接
    //当如实验数据  只有一家商户 华为，则不需要建立商户与用户的连接
    if (cust_id==""){
      unionRDD=relationShips
    }
    else{
      val whereNECon=cust_id+"!=\"\" and "+joinKey+"!=\"\""
      val groupCon="group by "+cust_id+","+joinKey
      val conCustDF=spark.sql("select %s,%s,count(1) as weight from %s where %s %s".format(cust_id,joinKey,payOrderName,whereNECon,groupCon))
      //TODO weight 1.0  由于暂时只有一家商户 华为，因此暂时不需要商户与用户的连接
      val relationCustCon:RDD[Edge[Double]]=conCustDF.rdd.map{x:org.apache.spark.sql.Row=>Edge(x.getAs[String](0).toLong,x.getAs[String](1).toLong,x.getAs[Long](2).toDouble )}
      //将用户图 与商户用户图结合建图
      unionRDD=relationShips.union(relationCustCon)
    }


    val graph=Graph.fromEdges(unionRDD,1)

    //例：根据订单号找出对应的盗号账户，从而建立新图

    val blackDF=spark.sql("select b.%s from %s a join %s b on a.%s=b.%s".format(joinKey,
      blackOrderName,payOrderName,black_key,black_key))
    val blackRdd=blackDF.rdd.map{x:org.apache.spark.sql.Row => x.getAs[String](0).toLong}
    val blackSet=blackRdd.collect
    //黑名单成员数
    val blackNum=blackSet.length

    val newVertices= graph.vertices.map{case(id,attr) => if(blackSet.contains(id) ) (id,1L) else (id,id) }
    val acBlackNum=newVertices.filter{case(id,attr) => attr==1L}.count

    val newGraph=Graph(newVertices,unionRDD)
    val louvainGraph=Graph.fromEdges(unionRDD,None)
    //LPA  检测团体
    val sc=spark.sparkContext

    val LouvainG=Louvain.execute(sc, louvainGraph,5,2)
    //LouvainG.vertices.map{case(id,attr) => attr.commVertices}
   // val result=BMLabelPropagation.run(newGraph,5)
    //result.vertices.foreach(println)
    /*
    val blackMaxGraph=result.mapVertices( (id,attr) =>  {val blackProb=attr.getOrElse(1L,0.0);
      if (blackProb>0.0) {(1L,blackProb)}
      else {val maxId=attr.maxBy(_._2)._1;(maxId,attr.get(maxId)  )}
    }  )

    //extract black vertice and its prob if without black then extract max

    val filterGraph=blackMaxGraph.subgraph(vpred = (id,attr) => attr._1==1L)
    filterGraph.vertices.foreach(println)
    val greyList=filterGraph.vertices

    //#############################
    val cc=filterGraph.connectedComponents().vertices
    val groupVetdf=cc.map{case(id,attr)=>(id.toLong,attr.toLong)}.toDF()
    groupVetdf.createOrReplaceTempView("groupVetTable")
    val attrWithKey=spark.sql("select distinct a.%s,%s,b._2 from %s a join groupVetTable b on a.%s=b._1".format(joinKey,simAttrL,payOrderName,joinKey))


    val keys=cc.map{case(id,attr)=>id}
    val allNum=cc.collect().length
    val groupNum=cc.map{case(id,attr)=>attr}.collect().toSet.size
    val groups=cc.map{case(id,attr)=>(attr,1)}
    val groupsArray=groups.reduceByKey((x,y)=>x+y).collect
    //println("groupNum:"+groupNum)
    //result.vertices.collect.foreach(println(_))
    //记录下图的边与顶点
    var writer = new PrintWriter(new File("edges.txt"))
    filterGraph.edges.collect().foreach(writer.println(_))
    writer.close()
    writer = new PrintWriter(new File("vertices.txt"))
    filterGraph.vertices.collect().foreach(writer.println(_))
    writer.close()

    writer = new PrintWriter(new File("report.txt"))
    writer.println("groupNum:"+groupNum)
    writer.println("blackNum:"+blackNum)
    writer.println("acBlackNum:"+acBlackNum)
    writer.println("allNum:"+allNum)
    writer.println("blackNum percent:"+acBlackNum*1.0/allNum)
    //TODO 风险类型比
    //各group成员数
    //TODO 核心成员，成员类型,关联密集度(边数除以点数)
    for(i<-Range(0,groupNum)){
      writer.println(groupsArray(i))
    }
    writer.println()
    writer.close()

    writer = new PrintWriter(new File("debugComponet.txt"))

    attrWithKey.rdd.collect().foreach(writer.println)
    writer.close()

    writer = new PrintWriter(new File("greyList.txt"))
    greyList.collect().foreach(writer.println)
    writer.close()

  */
   // val graphStream:MultiGraph = new MultiGraph("GraphStream")


  }
}