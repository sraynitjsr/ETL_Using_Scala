import java.io.File
import com.github.tototoshi.csv._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import scala.util.{Try, Success, Failure}
import java.util.Properties

object KafkaETL {

  case class Person(name: String, age: Int, city: String)

  def extract(filePath: String): Try[List[Person]] = {
    Try {
      val reader = CSVReader.open(new File(filePath))
      val data = reader.allWithHeaders().map { row =>
        Person(row("name"), row("age").toInt, row("city"))
      }
      reader.close()
      data
    }
  }

  def transform(data: List[Person]): List[Person] = {
    data.map(person => person.copy(age = person.age + 1))
  }

  def sendToKafka(topic: String, data: List[Person], kafkaProperties: Properties): Unit = {
    val producer = new KafkaProducer[String, String](kafkaProperties)
    data.foreach { person =>
      val record = new ProducerRecord[String, String](topic, person.name, person.toString)
      producer.send(record)
    }
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: KafkaETL <input_file_path> <kafka_topic> <kafka_bootstrap_servers>")
      return
    }

    val filePath = args(0)
    val topic = args(1)
    val kafkaBootstrapServers = args(2)

    val kafkaProperties = new Properties()
    kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
    kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    extract(filePath) match {
      case Success(extractedData) =>
        println("Extracted Data:")
        extractedData.foreach(println)

        val transformedData = transform(extractedData)
        println("\nTransformed Data:")
        transformedData.foreach(println)

        sendToKafka(topic, transformedData, kafkaProperties)
        println(s"Data sent to Kafka topic: $topic")

      case Failure(exception) =>
        println(s"Failed to extract data: ${exception.getMessage}")
    }
  }
}
