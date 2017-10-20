package project
{
    import utils._

    object Main {
        def main(args: Array[String]): Unit = {
            val (cmd, input, output) = (args(0),args(1),args(2))

            cmd match {
                case "netflow" | "cusmoc" | "cusflowsta" | "cusepiadv"
                    => runJobs(cmd).run(input, output)
                case _
                    => println("[ERROR]: No such job.")
            }
        }

        def runJobs(cmd: String): Jobs = cmd match {
            case "netflow"      => NetFlow
            case "cusmoc"       => CusModelCluster
            case "cusflowsta"   => CusFlowStA
            case "cusepiadv"    => CusEpPlA
        }
    }
}
