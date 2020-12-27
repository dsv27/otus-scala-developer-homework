package ru.otus.jdbc.overview

import java.sql.DriverManager

class ScalaJDBCTest extends PgTestContainer {


  "test example of PrepareStatement in ScalaJDBC" in {
    Class.forName(container.driverClassName)

    val connection = DriverManager.getConnection(container.jdbcUrl, container.username, container.password)

    val name = "ivan"
    val passw = "123123"

    val statement = connection
      .prepareStatement("select * from USERS where NAME = ? and PASSW = ?")

    statement.setString(1, name)
    statement.setString(2, passw)

    val set = statement.executeQuery()

    set.next()

    val resultName = set.getString(1)
    val resultPassw = set.getString(2)


    assert(name == resultName)
    assert(passw == resultPassw)


  }
}