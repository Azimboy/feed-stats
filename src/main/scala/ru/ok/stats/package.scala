package ru.ok

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

package object stats {

  case class Owners(
    user: Seq[Long],
    group: Seq[Long]
  )

  case class Resources(
    USER_PHOTO: Seq[Long],
    GROUP_PHOTO: Seq[Long],
    MOVIE: Seq[Long],
    POST: Seq[Long]
  )

  case class Feed(
    userId: Long,
    platform: String,
    durationMs: Long,
    position: Option[Int],
    timestamp: Option[Long],
    owners: Option[Owners],
    resources: Option[Resources]
  )

//  Для валидаций данных можно добавить лист правил по полям и последовательно можно проверить.
//  В этом случае у нас нет валидаторов для данных, но мы десериализуем всю таблицу и это узкая проверка данных.
  val feedsSchema: StructType = ScalaReflection.schemaFor[Feed].dataType.asInstanceOf[StructType]

}
