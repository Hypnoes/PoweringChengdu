package project.utils
{
    import org.apache.spark.sql.SparkSession

    case object CusFlowStA extends Jobs {
        override def run(input:String, output:String): Unit = {
            val spark = SparkSession
                .builder
                .appName(s"${this.getClass.getSimpleName}")
                .getOrCreate()
            import spark.implicits._

            val df = spark.read.options(Map("seq" -> "\001")).csv(input)

            val dsa = df.select($"_c2", $"_c1".cast("double"))
                        .withColumnRenamed("_c2", "date")
                        .withColumnRenamed("_c1", "value")
                        .as[User]
                        .map(x => User(Convert.toDate(x.date.toString.split(" ").apply(0)), x.value))
                        .groupBy("date").avg("value").sort("date")

            val arr = dsa.select("avg(value)").as[Double].collect
            val dif = {arr.tail :+ arr.head}.zip(arr).map(x => x._1 - x._2).dropRight(1)
            dif.map(x => if (x * 0.2 > 
                dif(if (dif.indexOf(x) == 0) 0 else dif.indexOf(x) - 1 )) 
                (x, "x") else (x, "o"))

            dif.toList.toDS.write.json(output)

            spark.stop()
        }

        case class User (date: java.sql.Date, value: Double)
    }

    case object CusFlowStA_V2 extends Jobs {
        override def run(input: String, output: String): Unit = {
            val spark = SparkSession
                        .build
                        .appName(s"${this.getClass.getSimpleName}")
                        .getOrCreate()
            import spark.implicits._

            val df = spark.read.options(Map("seq" -> "\001")).csv(input)

            val trans = df.select($"_c2", $"_c1".cast("double"))
                            .withColumnRenamed("_c2", "date")
                            .withColumnRenamed("_c1", "value")
                            .as[User]
                            .map(x -> User(Convert.toDate(x.date), x.value))
                            .groupBy("date").avg("")

            spark.stop
        }
    }
}
