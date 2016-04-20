package mobius

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by tawan on 4/19/2016.
  */
class TestListStatus {

  def run(input: String): Unit = {
    // val input = "cosmos://Cosmos09-Prod-Co3c/vol2/tmp/mobius/data/people.json"
    val sparkConf = new SparkConf().setAppName("TestListStatus")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json(input)
    df.printSchema()
    df.show()
    sc.stop()
  }

}
