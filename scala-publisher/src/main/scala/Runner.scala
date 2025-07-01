import User.encoder
import zio.json.EncoderOps
import zio.{RIO, ZIO}

object Runner {
  val forever: RIO[Publisher with Configuration, Unit] = for {
    configuration <- ZIO.service[Configuration]
    _             <- ZIO.foreachParDiscard(1 to configuration.application.parallelism)(_ => effect.forever)
  } yield ()

  private def effect: RIO[Publisher, Unit] = for {
    entity <- ZIO.succeed(User.next.toJson)
    _      <- Publisher.publish(entity)
  } yield ()
}
