/**
  * Created by think on 2017/4/13.
  */
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.SparkSession

object randomForest {
  def main(args:Array[String]) {
    val spark = SparkSession.builder().appName(args(0)).config("spark.some.config.option", "some_value").getOrCreate()
    val dataTrain = spark.read.format("libsvm").load("/home/month_6.txt")
    val dataTest = spark.read.format("libsvm").load("/home/month_7.txt")
    val data = spark.read.format("libsvm").load("/home/month_all.txt")
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)

    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)


    val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(50).setMaxDepth(7).setFeatureSubsetStrategy("sqrt").setSubsamplingRate(0.3)

    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val model = pipeline.fit(dataTrain)

    var predictions = model.transform(dataTrain)
    predictions.createOrReplaceTempView("predTable")
    //predictions.printSchema()

   // spark.sql(("select label,if(probability[1]>%f ,1,0) as predictedLabel from" +
   //   " predTable").format(args(1).toDouble)).write.mode("overwrite").parquet("tempParquet")
   // val midDF=spark.read.parquet("tempParquet");
   // midDF.createOrReplaceTempView("predTable");

    var trueNum=spark.sql("select count(*) as cnt from predTable where label=1.0").select("cnt").first.getLong(0)
    var predNum=spark.sql("select count(*) as cnt from predTable where predictedLabel=1.0").select("cnt").first.getLong(0)
    var rightNum=spark.sql("select count(*) as cnt from predTable where predictedLabel=label and label=1.0").select("cnt").first.getLong(0)
    println("recall")
    println((rightNum*1.0)/trueNum)
    println("precise")
    println((rightNum*1.0)/predNum)

     predictions = model.transform(dataTest)
    predictions.createOrReplaceTempView("predTable")
     trueNum=spark.sql("select count(*) as cnt from predTable where label=1.0").select("cnt").first.getLong(0)
     predNum=spark.sql("select count(*) as cnt from predTable where predictedLabel=1.0").select("cnt").first.getLong(0)
     rightNum=spark.sql("select count(*) as cnt from predTable where predictedLabel=label and label=1.0").select("cnt").first.getLong(0)
    println("recall")
    println((rightNum*1.0)/trueNum)
    println("precise")
    println((rightNum*1.0)/predNum)
    //val evaluator = new BinaryClassificationEvaluator().setLabelCol("indexedLabel").setRawPredictionCol("prediction").setMetricName("accuracy")
   // val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
  //  val accuracy = evaluator.evaluate(predictions)
    //println("Test Error = " + (1.0 - accuracy))

  }

}
