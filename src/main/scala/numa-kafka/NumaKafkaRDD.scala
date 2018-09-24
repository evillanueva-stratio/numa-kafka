package numa
/**
  * @author ${Edurne y Ernesto}
  */
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.TaskContext

object NumaKafkaRDD {

  def main(args : Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder().master("local").appName("numa-kafka").getOrCreate()
    import spark.implicits._
    implicit val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val sc = ssc.sparkContext
    sc.setLogLevel("ERROR")
    val conf = ConfigFactory.load()
    val kafkaServer = conf.getString("kafka.kafka-server")
    val groupId = conf.getString("kafka.group-id")
    val topic = List(conf.getString("kafka.topic"))
    val autoOffset = conf.getString("kafka.auto-offset")
    val autoCommit = conf.getString("kafka.auto-commit")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaServer,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> autoOffset,
      "enable.auto.commit" -> autoCommit
    )

    val inputStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )

    inputStream.foreachRDD { (rdd, time) =>
      println("Nuevo microbatch")
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println(offsets.toList.mkString("\n"))

      rdd.foreach { elem =>
        println(s"${elem.key} - ${elem.value} ")

      }
      println("")
      inputStream.asInstanceOf[CanCommitOffsets].commitAsync(offsets)

    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(Seconds(10000).milliseconds)

  }
}
