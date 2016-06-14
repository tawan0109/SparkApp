package spark

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.security.token.Token
import org.apache.spark._

import scala.math.random

object SparkPi {
  def main(args: Array[String]) {

    val tokenBytes = "TrustedApplication007".getBytes("UTF-8")
    val kind = new Text("TrustedApplicationTokenIdentifier")
    val trustAppToken = new Token(tokenBytes, "none".getBytes(), kind, kind)

    val creds = new Credentials()
    creds.addToken(kind, trustAppToken)
    //UserGroupInformation.getCurrentUser().addCredentials(creds)
    UserGroupInformation.getCurrentUser().addToken(kind, trustAppToken)

    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}

