import org.apache.kafka.clients.producer.ProducerRecord
import zio._
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde

trait Publisher {
  def publish[T](obj: T)(implicit serializer: T => String): Task[Unit]
}

object Publisher {
  def publish[T](obj: T)(implicit serializer: T => String): RIO[Publisher, Unit] =
    ZIO.serviceWithZIO[Publisher](_.publish(obj))
}

final case class PublisherImpl(producer: Producer, config: Configuration) extends Publisher {
  override def publish[T](obj: T)(implicit serializer: T => String): Task[Unit] = producer
    .produceAsync(new ProducerRecord[String, String](config.kafka.topic, serializer(obj)), Serde.string, Serde.string)
    //.tap(_ => ZIO.logInfo(s"Published ${obj.toString} on thread ${Thread.currentThread().threadId()}"))
    .catchAll(t => ZIO.logError(t.getMessage))
    .unit
}

object PublisherImpl {
  val live: RLayer[Configuration, PublisherImpl] = ZLayer.scoped {
    for {
      configuration <- ZIO.service[Configuration]
      producer <- Producer.make {
                    ProducerSettings(configuration.kafka.bootstrapServers)
                  }
    } yield PublisherImpl(producer, configuration)
  }
}
