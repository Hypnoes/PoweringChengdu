package project.utils

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{ GBTRegressionModel, GBTRegressor }

import org.apache.spark.sql.SparkSession

object CusEpPlA {
    def run(input:String, output:String): Unit = {
        val spark = SparkSession
            .builder
            .appName(s"${this.getClass.getSimpleName}")
            .getOrCreate()
        import spark.implicits._
        
        val df = spark.read.csv(input)
        
        val featureIndexer = new VectorIndexer()
            .setInputCol("features")
            .setOutputCol("indexedFeatures")
            .setMaxCategories(4)                // <-- {4}
            .fit(df)
        
        val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

        val gbt = new GBTRegressor()
            .setLabelCol("label")
            .setFeaturesCol("indexedFeatures")
            .setMaxIter(10)

        val pipeline = new Pipeline()
            .setStages(Array(featureIndexer, gbt))

        val model = pipeline.fit(trainingData)

        val predictions = model.transform(testData)

        predictions.select("prediction", "label", "features").write.csv(output)

        val evaluator = new RegressionEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("rmse")
        val rmse = evaluator.evaluate(predictions)
        println("root Mean Squared Error (RMSE) on test data = " + rmse)

        val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
        println("Learned regression GBT model:\n" + gbtModel.toDebugString)

        spark.stop()
    }
}

