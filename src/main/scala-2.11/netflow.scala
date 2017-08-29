package project.utils

import org.apache.spark.sql
import org.apache.spark.sql._

object NetFlow {
    def run(args: Array[String]): Unit = {
        val spark= SparkSession
            .builder
            .appName(s"${this.getClass.getSimpleName}")
            .getOrCreate()
        import spark.implicits._
        
        val root = "hdfs://10.170.31.120:9000/user/hypnoes/"
        val r = """((\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}(\s>\s)?){2}""".r 
        
        val raw = spark.read.text(root + "liuliang.txt").as[String]
        val df = raw.map(r.findFirstIn(_)).map(_.get.split(" > ")).map(x => (x(0), x(1)))
            .toDF("IN", "OUT").groupBy("IN", "OUT").count.sort($"Count".desc)

        df.write.json(root + "blas")

        spark.stop()
    }
}
