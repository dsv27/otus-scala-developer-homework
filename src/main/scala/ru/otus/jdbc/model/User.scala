package ru.otus.jdbc.model


import java.util.UUID

sealed trait Role
object Role {
  case object Reader  extends Role
  case object Manager extends Role
//  case object Manager2 extends Role
  case object Admin extends Role
}

case class User(
    id: Option[UUID],
    firstName: String,
    lastName: String,
    age: Int,
    roles: Set[Role]
)
