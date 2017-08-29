package project.utils

import scala.language.implicitConversions
import org.apache.spark.sql.SparkSession

object CusFlowStA {
    def run(args: Array[String]): Unit = {
        val spark = SparkSession
                    .builder
                    .appName(s"${this.getClass.getSimpleName}")
                    .getOrCreate()
        import spark.implicits._

        val root = "hdfs://10.170.31.120:9000/user/hypnoes/"
        val df = spark.read.csv(root + args(1))

        val dsa = df.select($"_c0", $"_c1".cast("double"))
                    .withColumnRenamed("_c0", "date")
                    .withColumnRenamed("_c1", "value")
                    .as[User]
                    .map(x => User(x.date.toString.split(" ").apply(0), x.value))
                    .groupBy("date").avg("value").sort("date")

        val arr = dsa.select("avg(value)").as[Double].collect
        val dif = {arr.tail :+ arr.head}.diff(arr).dropRight(1)                 // <- diff return empty
        dif.map(x => if (x * 0.2 > 
            dif(if (dif.indexOf(x) == 0) 0 else dif.indexOf(x) - 1 )) 
            (x, "x") else (x, "o"))                                             // <- type miss match

        dif.toList.toDS.write.json(root + args(2))
        
        spark.stop()
    }
            
    implicit def toTime(stringDate: String): java.sql.Date = {
        val sdf = new java.text.SimpleDateFormat("yyyy/MM/dd")    
        return new java.sql.Date(sdf.parse(stringDate).getTime())        
    }

    case class User (date: java.sql.Date, value: Double)
}
