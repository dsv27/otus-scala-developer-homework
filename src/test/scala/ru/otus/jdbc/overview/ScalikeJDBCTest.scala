package ru.otus.jdbc.overview

import scalikejdbc._


class ScalikeJDBCTest  extends PgTestContainer {

  "test interpolation" in {
    Class.forName(container.driverClassName)

    ConnectionPool.singleton(container.jdbcUrl, container.username, container.password)

    val name  = "ivan"
    val passw = "123123"

    case class Person(name: String, passw: String)
    case class Person1(name: String)

    val userNames = DB readOnly { implicit session =>
      sql"select NAME, PASSW from USERS where NAME = $name and PASSW = $passw"
        .map(rs =>
          Person(
            name = rs.string("NAME"),
            passw = rs.string("PASSW")
          )).list.apply()
    }

    assert(userNames.head.name == "ivan")
    assert(userNames.head.passw == "123123")
  }


  case class Person(name: String, passw: String)
//  case class Person1(name: String, passw: String)

  object Person extends SQLSyntaxSupport[Person] {
    override val tableName = "USERS"
    def apply(a: SyntaxProvider[Person])(rs: WrappedResultSet): Person = apply(a.resultName)(rs)
    def apply(a: ResultName[Person])(rs: WrappedResultSet): Person = new Person(
      name = rs.string(a.name),
      passw = rs.string(a.passw))
  }

  "test Query DSL" in {
    Class.forName(container.driverClassName)

    ConnectionPool.singleton(container.jdbcUrl, container.username, container.password)

    DB localTx { implicit s =>
      val pp = Person.syntax("p")

      val ivan: Person = withSQL(
        select
          .from(Person as pp)
          .where
          .withRoundBracket {
            _.eq(pp.name, "ivan")
              .and
              .eq(pp.passw, "123123")
          }
      )
        //      .map(Person1(pp)).single.apply().get
        .map(Person(pp)).single.apply().get

      assert(ivan.name == "ivan")
      assert(ivan.passw == "123123")
    }
  }
}