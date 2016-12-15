package flink.batch

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration


object ExampleDataSource {
  val PATH = "src/main/resources/"

  case class User(no: Int, name: String, addr: String)

  def main(args: Array[String]): Unit = {
    val env  = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // read text file from local files system
    val localLines = env.readTextFile(PATH + "log.txt")

    // read a CSV file with three fields
    val csvInput = env.readCsvFile[(Int, String, String)](PATH + "sample.csv")

    // read a CSV file with five fields, taking only two of them
    val csvInput2 = env.readCsvFile[(Int, String)](PATH + "sample.csv", includedFields = Array(0, 2)) // take the first and the fourth field

    // CSV input can also be used with Case Classes
    val csvInput3 = env.readCsvFile[User](PATH + "sample.csv")

    // read a CSV file with three fields into a POJO (Person) with corresponding fields
    val csvInput4 = env.readCsvFile[User](PATH + "sample.csv", pojoFields = Array("no", "name", "addr"))


    // create a set from some given elements
    val values = env.fromElements("Foo", "bar", "foobar", "fubar")
//
    // generate a number sequence
    val numbers = env.generateSequence(1, 1000);


    // recursive
    val parameters = new Configuration
    parameters.setBoolean("recursive.file.enumeration", true)

    val recursiveData = env.readTextFile(PATH).withParameters(parameters)

  }
}
