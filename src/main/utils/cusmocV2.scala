package project.utils
{
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.ml.clustering.KMeans
    import org.apache.spark.ml.linalg.Vectors

    case object CusModelClusterV2 extends Jobs {
        override def run(input:String, output:String): Unit = {
            val spark = SparkSession
                .builder
                .appName(s"${this.getClass.getSimpleName}")
                .getOrCreate()
            import spark.implicits._

            // pre-process the data
            val df = spark.read.option("header", true).csv(input)
            val raw = df.drop("ID", "DATA_DATE", "DATA_TYPE", "OBJECT_TYPE", "CATE_TYPE", "CATE_SUBTYPE", 
                                "DATA_POINT_FLAG", "DATA_WHOLE_FLAG")

            val fetSeq = raw.collect.map(x => Vectors.dense(x.toSeq.toArray.map(_.toString.toDouble))).toSeq
            val lebSeq = df.select("ID", "DATA_DATE").as[(String, String)].map(x => (x._1, Convert.toDate(x._2))).toDF("id", "date").as[Leab].collect.toSeq
            val tabSeq = lebSeq.zip(fetSeq).zipWithIndex
            val tab = spark.createDataset(tabSeq.map(x => Powertable(x._2, x._1._1.id, x._1._1.date, x._1._2)))


            // generate the label-feature

            //val clusModeV1 = tab.groupBy("date").count
            //val clusModeV2 = tab.groupBy("id").count
            
            val kmm = new KMeans().setK(3)
            val model = kmm.fit(tab)

            model.summary.cluster.show
            model.summary.predictions.show
            //val model1 = kmm.fit(clusModeV1)
            //model1.summary.predictions.write.json(output + "_1")

            //val model2 = kmm.fit(clusModeV2)
            //model2.summary.predictions.write.json(output + "_2")

            spark.stop()
        }
        
        case class Powertable (key: Int, id: String, date: java.sql.Date, features: org.apache.spark.ml.linalg.Vector)
        case class Leab (id: String, date: java.sql.Date)
    }
}
