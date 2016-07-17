
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import org.apache.spark.sql.cassandra._
import org.apache.log4j.{Level, Logger}

/**
 * Simple Spark Cassandra
 * One example with Scala case class marshalling
 * Another example using Spark SQL
 */
object SparkGoT {

  case class Battle(
    battle_number: Integer,
    year: Integer,
    attacker_king: Option[String],
    defender_king: Option[String])

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkCassandra_GoT")

    if (args.length > 0) conf.setMaster(args(0)) // in case we're running in sbt shell such as: run local[5]

    conf.set("spark.driver.port", "7001")
    conf.set("spark.fileserver.port", "7003")
    conf.set("spark.broadcast.port", "7004")
    conf.set("spark.replClassServer.port", "7005")
    conf.set("spark.blockManager.port", "7006")
    conf.set("spark.executor.port", "7007")
    conf.set("spark.cassandra.connection.host", "cassandra_3")

    val sc = new SparkContext(conf)
    
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)

    // Spark Cassandra Example one which marshalls to Scala case classes
    val battles = sc.cassandraTable[Battle]("gameofthrones", "battles").
      select("battle_number", "year", "attacker_king", "defender_king")
      .as((b: Int, y: Int, a: Option[String], d: Option[String]) => new Battle(b, y, a, d))

    battles.foreach { b: Battle =>
      println("Battle Number %s was defended by %s.".format(b.battle_number, b.defender_king))
    }

    // Spark Cassandra Example Two - Create DataFrame from Spark SQL read
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "battles", "keyspace" -> "gameofthrones"))
      .load()

    df.show

    // Game of Thrones Battle analysis 

    // Who were the most aggressive kings?  (most attacker_kings)
    val countsByAttack = df.groupBy("attacker_king").count().sort(desc("count"))
    countsByAttack.show()

    // Which kings were attacked the most?  (most defender_kings)
    val countsByDefend = df.groupBy("defender_king").count().sort(desc("count"))
    countsByDefend.show()

    sc.stop()

  }
}