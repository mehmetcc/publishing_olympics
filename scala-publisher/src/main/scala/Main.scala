import zio._

object Main extends ZIOAppDefault {
  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    program.provide(Configuration.live, PublisherImpl.live)

  private val program = Runner.forever
}
