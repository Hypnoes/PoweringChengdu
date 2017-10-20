package project.utils
{
    import org.apache.spark.sql._

    case object NetFlow extends Jobs {
        override def run(input: String, output: String): Unit = {
            val spark= SparkSession
                .builder
                .appName(s"${this.getClass.getSimpleName}")
                .getOrCreate()
            import spark.implicits._

            val r = """((\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}(\s>\s)?){2}""".r 
            
            val raw = spark.read.text(input).as[String]
            val df = raw.map(r.findFirstIn(_)).map(x => if(x.isEmpty) "- > -" else x.get).map(_.split(" > "))
                .map(x => (x(0), x(1))).toDF("IN", "OUT").groupBy("IN", "OUT").count.sort($"Count".desc)

            //def fusion(df:Dataset[Row]): Dataset[Row] = {
            //    df.where($"count" > 1000)
            //        .map(x => (x.getString(0).split('.'), x.getString(1).split('.')))
            //        .map(x => (
            //            if(x._1.length == 5) x._1.dropRight(1).:+("*") else x._1.:+("*"), 
            //            if(x._2.length == 5) x._2.dropRight(1).:+("*") else x._2.:+("*")))
            //        .map(x => (x._1.mkString("."), x._2.mkString(".")))
            //        .toDF("IN", "OUT")
            //}
            //
            //val df1 = fusion(df).groupBy("IN", "OUT").count.sort($"Count".desc)
            //val df2 = fusion(df1).groupBy("IN", "OUT").count.sort($"Count".desc)
            //val df3 = fusion(df2).groupBy("IN", "OUT").count.sort($"Count".desc)
            //val df4 = fusion(df3).groupBy("IN", "OUT").count.sort($"Count".desc)
            //val df5 = fusion(df4).groupBy("IN", "OUT").count.sort($"Count".desc)
            //
            //df5.show
            
            df.write.json(output)

            spark.stop()
        }
    }
}
