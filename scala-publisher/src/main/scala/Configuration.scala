import zio._
import zio.config._
import zio.config.magnolia.deriveConfig
import zio.config.typesafe.TypesafeConfigProvider

case class ApplicationConfiguration(parallelism: Int)

case class KafkaConfiguration(
  bootstrapServers: List[String],
  topic: String
)

case class Configuration(
  application: ApplicationConfiguration,
  kafka: KafkaConfiguration
)

object Configuration {
  private val appConfig: Config[ApplicationConfiguration] = deriveConfig[ApplicationConfiguration].nested("application")

  private val kafkaConfig: Config[KafkaConfiguration] = deriveConfig[KafkaConfiguration].nested("kafka")

  private val combinedConfig: Config[Configuration] = (appConfig zip kafkaConfig).to[Configuration]

  val live: Layer[Config.Error, Configuration] = ZLayer.fromZIO {
    ZIO
      .config[Configuration](combinedConfig)
      .withConfigProvider(TypesafeConfigProvider.fromResourcePath().kebabCase)
      .tap { configuration =>
        ZIO.logInfo(
          s"""
             |Configuration loaded:
             |  Application:
             |    Parallelism: ${configuration.application.parallelism}
             |  Kafka:
             |    Bootstrap Servers: ${configuration.kafka.bootstrapServers.mkString(",")}
             |    Topic: ${configuration.kafka.topic}
             |""".stripMargin
        )
      }
  }
}
