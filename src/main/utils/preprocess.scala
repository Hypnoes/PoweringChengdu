package project.utils 
{
    import org.apache.spark.sql.{ SparkSession, DataFrame }
    import org.apache.spark.ml.linalg.Vectors
    
    case object PreProcess extends Jobs {
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
            val lebSeq = df.select("ID", "DATA_DATE")
                            .as[(String, String)]
                            .map(x => (x._1, Convert.toDate(x._2)))
                            .toDF("id", "date").as[Leab].collect.toSeq
            val tabSeq = lebSeq.zip(fetSeq).zipWithIndex
            val tab = spark.createDataset(tabSeq.map(x => Powertable(x._2, x._1._1.id, x._1._1.date, x._1._2)))

            tab.write.csv(output)
        }

        /* 
         * We want data for training KMeans model
         * Input format:
         * ID DATA_DATE DATA_TYPE OBJECT_TYPE DATA_POINT_FLAG DATA_WHOLE_FLAGS P1 ... P96
         * 
         * Output format:
         * id:String date:java.sql.date features:org.apache.spark.ml.linalg.Vector
         */ 
        def readDataForKMeans(input: String): DataFrame = { 
            val spark = SparkSession
                        .builder
                        .appName(s"${this.getClass.getSimpleName}")
                        .getOrCreate()
            import spark.implicits._

            // pre-process the data
            val df = spark.read.options(Map(
                        "header"     -> "true", 
                        "sep"        -> ",",
                        "dateFormat" -> "yyyy/MM/dd"
                        )).csv(input)

            val raw = df.drop("ID", "DATA_DATE", "DATA_TYPE", "OBJECT_TYPE", "CATE_TYPE", "CATE_SUBTYPE", 
                                "DATA_POINT_FLAG", "DATA_WHOLE_FLAG")

            val fetSeq = raw.collect.map(x => Vectors.dense(x.toSeq.toArray.map(_.toString.toDouble))).toSeq
            val lebSeq = df.select("ID", "DATA_DATE")
                            .as[(String, String)]
                            .map(x => (x._1, Convert.toDate(x._2)))
                            .toDF("id", "date").as[Leab].collect.toSeq
            val tabSeq = lebSeq.zip(fetSeq).zipWithIndex
            val tab = spark.createDataset(tabSeq.map(x => Powertable(x._2, x._1._1.id, x._1._1.date, x._1._2)))

            return tab.toDF
        }

        /*
         *
         */

        //def readDataForGBTR(input: String): DataFrame = { }

        case class Powertable (key: Int, id: String, date: java.sql.Date, features: org.apache.spark.ml.linalg.Vector)
        case class Leab (id: String, date: java.sql.Date)

    }
}
