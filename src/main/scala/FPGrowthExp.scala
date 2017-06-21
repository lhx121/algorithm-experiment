import java.io._

import org.apache.spark.annotation.Since
import scopt.OptionParser
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row


/**
  * @author RoyGao
  */

object FPGrowthExp {
  case class Params(
                     input: String = null,
                     minSupport: Double = 0.01,
                     //minConfidence: Double = 0.01,
                     fields:Seq[String] = Seq(),
                     numPartition: Int = -1
                   ) extends AbstractParams[Params]
  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("FPGrowthExample") {
      head("FPGrowth: an example FP-growth app.")
      opt[Double]("minSupport")
        .text(s"minimal support level, default: ${defaultParams.minSupport}")
        .action((x, c) => c.copy(minSupport = x))
//      opt[Double]("minConfidence")
//        .text(s"minimal confidence level, default: ${defaultParams.minConfidence}")
//        .action((x, c) => c.copy(minConfidence = x))
      opt[Int]("numPartition")
        .text(s"number of partition, default: ${defaultParams.numPartition}")
        .action((x, c) => c.copy(numPartition = x))
      opt[Seq[String]]('f', "fields").valueName("<fiels1>,<field2>...").action( (x,c) =>
        c.copy(fields = x) ).text("fields to include")
      arg[String]("<input>")
        .text("input paths to input data set, whose file format is that each line " +
          "contains a transaction with each item in String and separated by a space")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSession")
      .enableHiveSupport()
      .getOrCreate()

    val query = params.fields.mkString("select ", ",", s" from ${params.input}")+" where "+params.fields.mkString(" is not null and ")+ " is not null"
    val transactions = spark.sql(query).rdd.map{ x:Row =>
      val array = x.toSeq.toArray.filter(_ != "").map(_.toString);array}

    println(s"Number of transactions: ${transactions.count()}")

    val model = new FPGrowth()
      .setMinSupport(params.minSupport)
      .setNumPartitions(params.numPartition)
      .run(transactions)

    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")

    val candidates = model.freqItemsets.flatMap { itemset =>
      val items = itemset.items
      items.flatMap { item =>
        items.partition(_ == item) match {
          case (c,a) if !a.isEmpty =>
            Some((a.toSeq, (c.toSeq, itemset.freq)))
          case _ => None
        }
      }
    }

    val rawRules = candidates.join(model.freqItemsets.map(x => (x.items.toSeq, x.freq)))
      .map {case (a, ((c, fU), fA)) => (c(0), (a.toArray, fU, fA))}

    val consFreq = model.freqItemsets.filter(x => x.items.length == 1)
      .map(x => (x.items(0), x.freq))

    val sampleNum = transactions.count();

    val res = rawRules.join(consFreq).map {
      case (c, ((a, fU, fA), fW)) =>
        new Rule[String](a.toArray, Array(c), fU, fA, fW, sampleNum)
      }//.filter(_.confidence >= params.minConfidence)
    val writer = new PrintWriter(new File("rule.txt"))
    res.collect().foreach{ rule =>
      writer.println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence, + rule.lift);
    }
    writer.close()

    spark.stop()
  }

  class Rule[Item] (
                                  val antecedent: Array[Item],
                                  val consequent: Array[Item],
                                  freqUnion: Double,
                                  freqAntecedent: Double,
                                  freqConsequent: Double,
                                  freqAll: Double) extends Serializable {

    /**
      * Returns the confidence of the rule.
      *
      */
    def confidence: Double = freqUnion.toDouble / freqAntecedent

    /**
      * Returns the Lift of the rule.
      *
      */
    def lift: Double = (freqUnion.toDouble / freqAntecedent) / (freqConsequent / freqAll)

    require(antecedent.toSet.intersect(consequent.toSet).isEmpty, {
      val sharedItems = antecedent.toSet.intersect(consequent.toSet)
      s"A valid association rule must have disjoint antecedent and " +
        s"consequent but ${sharedItems} is present in both."
    })


    override def toString: String = {
      s"${antecedent.mkString("{", ",", "}")} => " +
        s"${consequent.mkString("{", ",", "}")}: ${confidence}"
    }
  }
}
