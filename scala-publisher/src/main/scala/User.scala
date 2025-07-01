import com.github.javafaker.Faker
import zio.json.{DeriveJsonEncoder, JsonEncoder}

case class User private (firstName: String, lastName: String, address: String)

object User {
  def next: User = {
    val faker = new Faker()

    val name    = faker.name()
    val address = faker.address().fullAddress()

    User(name.firstName(), name.lastName(), address)
  }

  implicit val encoder: JsonEncoder[User] = DeriveJsonEncoder.gen[User]
}
