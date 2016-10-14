import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import kafka.serializer.{DefaultDecoder, StringDecoder}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

object KafkaWordCount {
  def main(args: Array[String]) {

    val kafkaConf = Map(
	"metadata.broker.list" -> "localhost:9092",
	"zookeeper.connect" -> "localhost:2181",
	"group.id" -> "kafka-spark-streaming",
	"zookeeper.connection.timeout.ms" -> "1000")

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf , Seconds(2))
    ssc.checkpoint("checkpoint")

    // if you want to try the receiver-less approach, comment the below line and uncomment the next one
  // val messages = KafkaUtils.createStream[String, String, DefaultDecoder, StringDecoder](ssc, kafkaConf , Set("avg") )
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf , Set("avg") )
val values = messages.map(x => x._2.split(","))
val pairs = values.map(x => (x(0).toString ,  x(1).toInt))



val mappingFunc = (key: String, one: Option[Int], sums: State[(Int, Int)] ) => {
val sum = one.getOrElse(0) + sums.getOption.getOrElse((0,0))._1
val count = 1 + sums.getOption.getOrElse((0,0))._2
sums.update((sum, count))
// we are returning the sum and count for validation
(key, sum, count , (sum.toFloat/count) )
}

val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc))

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
