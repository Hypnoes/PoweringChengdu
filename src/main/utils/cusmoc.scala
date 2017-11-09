package project.utils
{
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.ml.clustering.KMeans

    case object CusModelCluster extends Jobs {
        override def run(input:String, output:String): Unit = {
            val spark = SparkSession
                .builder
                .appName(s"${this.getClass.getSimpleName}")
                .getOrCreate()
            import spark.implicits._

            val df = spark.read.csv(input)
            val kmm = new KMeans().setK(3)
            val model = kmm.fit(df)
            val ce = model.clusterCenters.map(item => bc(item)).toList
            val centerDS = spark.createDataset(ce)
            
            model.transform(centerDS).write
                .json(output + "_center")
            model.transform(df).write
                .json(output)

            spark.stop()
        }

        case class bc (clusterCenters: org.apache.spark.ml.linalg.Vector)
    }
}
