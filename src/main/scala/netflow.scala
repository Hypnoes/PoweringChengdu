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
            val df = raw.map(r.findFirstIn(_)).map(_.get.split(" > ")).map(x => (x(0), x(1)))
                .toDF("IN", "OUT").groupBy("IN", "OUT").count.sort($"Count".desc)

            df.write.json(output)

            spark.stop()
        }
    }
}
