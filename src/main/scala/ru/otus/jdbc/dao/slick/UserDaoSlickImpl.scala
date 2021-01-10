package ru.otus.jdbc.dao.slick
import ru.otus.jdbc.model.{Role, User}

import slick.dbio.Effect
import slick.jdbc.PostgresProfile.api._
import slick.sql.FixedSqlAction

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import slick.dbio.Effect.Read

class UserDaoSlickImpl(db: Database)(implicit ec: ExecutionContext) {
  import UserDaoSlickImpl._

  def getUser(userId: UUID): Future[Option[User]] = {
    val res = for {
      user <- users.filter(user => user.id === userId).result.headOption
      roles <- usersToRoles
        .filter(_.usersId === userId)
        .map(_.rolesCode)
        .result
        .map(_.toSet)
    } yield user.map(_.toUser(roles))

    db.run(res)
  }
//createUser - метод должен делать вставки пользователя
  def createUser(user: User): Future[User] = {

    val userCreate = (for {
      userId <- (users returning users.map(_.id)) += UserRow(
        id = None,
        firstName = user.firstName,
        lastName = user.lastName,
        age = user.age
      )
      toRole <- usersToRoles ++= user.roles.map(userId -> _)
    } yield user.copy(id = Some(userId))).transactionally

    db.run(userCreate)
  }

  /*updateUser - необходимо доработать метод, так чтобы action выполнялись в одной транзакции и чтобы в случае обновления несуществующего пользователя происходила вставка.
   * Сделать Lock на операцию обновления.
   * .insertOrUpdate((user.firstName, user.lastName, user.age)) - не взлетело, так ккак автоинкремент в id
   */
  def updateUser(user: User): Future[Unit] = {
    user.id match {
      case Some(userId) =>
        val existUserQ = users.filter(_.id === user.id)
        val existUserA = existUserQ.exists.result
        val insertOrUpdateA = existUserA.flatMap {
          case true => {
            val updateUserQ = users
              .filter(_.id === userId)
              .map(u => (u.firstName, u.lastName, u.age))
              .update((user.firstName, user.lastName, user.age))

            val deleteRolesQ = usersToRoles.filter(_.usersId === userId).delete
            val insertRolesQ = usersToRoles ++= user.roles.map(userId -> _)
            val uId = userId.toString()
            val lockUserQ =
              sql"select 1 from users where id::text = $uId for update"
                .as[Int] // Не совсеми видами БД совместимо, ну и не красиво

            lockUserQ >> updateUserQ >> deleteRolesQ >> insertRolesQ >> DBIO
              .successful(())
          }
          case false => {
            createUser(user)
            DBIO.successful(())
          }
        }
        db.run(
          insertOrUpdateA.transactionally
            .withTransactionIsolation(
              slick.jdbc.TransactionIsolation.Serializable
            ) // Так более совместимо но в высоконкуретной среде тормозно)
        )
      case None =>
        createUser(user)
        Future.successful(())
    }
  }
  /*
deleteUser - метод должен удалять пользователя из базы и возвращать удаленного пользователя, с учетом того, что его может не быть
   */
  def deleteUser(userId: UUID): Future[Option[User]] = {
    getUser(userId).flatMap {
      case None => Future.successful(None)
      case Some(u) =>
        val deleteUser = users.filter(_.id === userId).delete
        val deleteRoles = usersToRoles.filter(_.usersId === userId).delete
        val action =  deleteRoles >> deleteUser >> DBIO.successful(Some(u))
        db.run(action.transactionally)
    }
  }
  /*
findByCondition - метод должен возвращать массив пользователей, выбранных по условию, переданному в качестве параметра
   */
  def findByCondition(
      condition: Users => Rep[Boolean]
  ): Future[Vector[User]] = {
    // Делаем левое соединение User и user_to_roles, можно сделать и полное, но левое позволит вернуть пользаков без ролей
    val userAndRoles = for {
      (user, userRoles) <- users
        .filter(condition)
        .joinLeft(usersToRoles)
        .on(_.id === _.usersId)
    } yield (user, userRoles.map(_.rolesCode))

    // Групируем по Users, и запихиваем Roles в Set(Roles)
    val res = for {
      userAndGroupGrouped <- userAndRoles.result
    } yield userAndGroupGrouped
      .groupBy(_._1)
      .map({ case (user, rowsGrouped) =>
        user.toUser(rowsGrouped.flatMap(_._2.toSet).toSet)
      })
      .toVector

    db.run(res)
  }
  /*
findByLastName - метод должен возвращать массив пользователей с заданной фамилией
   */
  def findByLastName(lastName: String): Future[Seq[User]] = {
    findByCondition(_.lastName === lastName)
  }

  def findAll(): Future[Seq[User]] = findByCondition(_ => true)

  def deleteAll(): Future[Unit] = {

    val res = for {
      _ <- usersToRoles.delete
      _ <- users.delete
    } yield ()
    db.run(res.transactionally)
  }
}

object UserDaoSlickImpl {
  implicit val rolesType: BaseColumnType[Role] =
    MappedColumnType.base[Role, String](
      {
        case Role.Reader  => "reader"
        case Role.Manager => "manager"
        case Role.Admin   => "admin"
      },
      {
        case "reader"  => Role.Reader
        case "manager" => Role.Manager
        case "admin"   => Role.Admin
      }
    )

  case class UserRow(
      id: Option[UUID],
      firstName: String,
      lastName: String,
      age: Int
  ) {
    def toUser(roles: Set[Role]): User =
      User(id, firstName, lastName, age, roles)
  }

  object UserRow extends ((Option[UUID], String, String, Int) => UserRow) {
    def fromUser(user: User): UserRow =
      UserRow(user.id, user.firstName, user.lastName, user.age)
  }

  class Users(tag: Tag) extends Table[UserRow](tag, "users") {
    val id = column[UUID]("id", O.PrimaryKey, O.AutoInc)
    val firstName = column[String]("first_name")
    val lastName = column[String]("last_name")
    val age = column[Int]("age")

    val * = (id.?, firstName, lastName, age).mapTo[UserRow]
  }

  val users: TableQuery[Users] = TableQuery[Users]

  class UsersToRoles(tag: Tag)
      extends Table[(UUID, Role)](tag, "users_to_roles") {
    val usersId = column[UUID]("users_id")
    val rolesCode = column[Role]("roles_code")

    val * = (usersId, rolesCode)
  }

  val usersToRoles = TableQuery[UsersToRoles]
}
