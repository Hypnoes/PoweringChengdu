package project

import utils._

object Main {
    def main(args: Array[String]): Unit = {
        val (cmd, input, output) = args(1,2,3)

        cmd match {
            case "InThisCase"            // << TO DO HERE.
                => Jobs(cmd).run(input, output)
            case _
                => println("[ERROR]: No such job.")
        }
    }
}
