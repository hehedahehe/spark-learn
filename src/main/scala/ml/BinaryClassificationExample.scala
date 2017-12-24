package ml

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

/*
 * @desc
 * @author lirb
 * @datetime 2017/12/21,16:03
 */
object BinaryClassificationExample {

  def main(args: Array[String]): Unit = {

    val dataPath = "E:\\WORK\\DATASET\\sparkExamples\\data\\mllib\\sample_libsvm_data.txt"

    val spark = SparkSession.builder()
      .appName("BinaryClassificationMetricsExample").getOrCreate()

    val data = spark.read.format("libsvm").load(dataPath)
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    training.cache()


    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val lrModel = lr.fit(training)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")


    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(training)

    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

    spark.stop()

  }
}
