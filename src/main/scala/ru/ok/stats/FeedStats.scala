package ru.ok.stats

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object FeedStats {

  import Utils._

  val FeedsTable = "feeds"

  def main(args: Array[String]): Unit = {
    val session: SparkSession = createSparkSession(this.getClass.getName)
//    First time we save base table. Partitioning by date and saving as Feeds table
//    Run this only once
//    saveFeedsTable(session)

//    After saving base table Feeds (saveFeedsTable()), we can load it more time for building another reports
    session.loadTableFromOrcFile(FeedsTable)

//  a. Количество показов и уникальных пользователей за день
//     в разрезе по платформам, в том числе по всем платформам суммарно;
//    Cложность алгоритма O(nlog(n))
//    countViewsAndUniqueUsersByDate(session)

//  b. Количество за день уникальных авторов и уникального контента, показанного в ленте;
//    Cложность алгоритма O(nlog(n))
//    countUniqueAuthorsAndContentsByDate(session)

//  c. Количество сессий, средняя глубина просмотра (по позиции фида)
//     и средняя продолжительность пользовательской сессии в ленте за день.
//    Cложность алгоритма O(nlog(n))
//    countSessionAndAvgViewByDate(session)

//  d. Дополнительно дан список пользователей в формате <userId: Long>,
//     включающий в себе несколько тысяч пользователей. Нужно посчитать количество
//     показов фидов в ленте по этим пользователям.
//    Cложность алгоритма O(nlog(n))
    countFeedsByUser(session)
  }

  def saveFeedsTable(session: SparkSession): Unit = {
    import session.implicits._
    session.readTextFile("feeds_show.json")
      .withColumn("data", from_json($"value", feedsSchema))
      .select("data.*")
      .withColumn("timestamp", to_utc_timestamp(from_unixtime($"timestamp" / 1000), "GMT+3"))
      .withColumn("date", to_date($"timestamp", "yyyy-MM-dd"))
      .orderBy("date")
      .saveWithRepartition(FeedsTable, "date")
  }

  def countViewsAndUniqueUsersByDate(session: SparkSession): Unit = {
    val feeds = session.table(FeedsTable)
      .select("userId", "platform", "date")

    val countsByPlatform = feeds.groupBy("date", "platform")
      .pivot("platform")
      .agg(
        count("*").as("cnt_view"),
        countDistinct("userId").as("cnt_unique_user"),
      ).na.fill(0)
      .drop("platform")

    val columns = countsByPlatform.columns.filterNot(_ == "date").map(col => sum(col).as(col.toLowerCase))
    val countsByDate = countsByPlatform
      .groupBy("date")
      .agg(columns.head, columns.tail: _*)
      .withColumn("total_cnt_view", countsByPlatform.columns.filter(_.endsWith("cnt_view")).map(col).reduce(_ + _))
      .withColumn("total_cnt_unique_user", countsByPlatform.columns.filter(_.endsWith("cnt_unique_user")).map(col).reduce(_ + _))

    countsByDate.show(10, false)
  }

  def countUniqueAuthorsAndContentsByDate(session: SparkSession): Unit = {
    import session.implicits._
    val addSalt = udf((salt: String, arr: Seq[String]) => {arr.map(salt + ":" + _)})
    val distinctCount = udf((arrs: Seq[Seq[String]]) => arrs.flatten.distinct.size)

    session.table(FeedsTable)
      .filter($"owners.user".isNotNull || $"owners.group".isNotNull)
      .select($"date",
        array_union(
          addSalt(lit("user"), coalesce($"owners.user", array())),
          addSalt(lit("group"), coalesce($"owners.group", array()))
        ).as("owners"),
        flatten(array(
          addSalt(lit("user_photo"), coalesce($"resources.USER_PHOTO", array())),
          addSalt(lit("group_photo"), coalesce($"resources.GROUP_PHOTO", array())),
          addSalt(lit("movie"), coalesce($"resources.MOVIE", array())),
          addSalt(lit("post"), coalesce($"resources.POST", array()))
        )).as("contents")
      ).groupBy("date")
      .agg(
        distinctCount(collect_set("owners")),
        distinctCount(collect_set("contents"))
      )
      .show(10, false)
  }

  def countSessionAndAvgViewByDate(session: SparkSession): Unit = {
    import session.implicits._

    val sessions = session.table(FeedsTable)
      .select("userId", "timestamp", "date")
      .withColumn("diff", ($"timestamp".cast(LongType) - lag("timestamp", 1)
        .over(Window.partitionBy("userId").orderBy("timestamp")).cast(LongType)) / 60)
      .filter($"diff" > 15 || $"diff".isNull)
      .select(
        $"userId",
        row_number()
          .over(Window.partitionBy("userId").orderBy("timestamp"))
          .as("sessionId"),
        $"timestamp".as("startedAt"),
        lead($"timestamp", 1)
          .over(Window.partitionBy("userId").orderBy("timestamp"))
          .as("nextStartedAt"),
        $"date"
      )
      .as("session")

    sessions.show(100)

    val feeds = session.table(FeedsTable)
      .select("userId", "durationMs", "position", "timestamp")
      .filter($"position".isNotNull)
      .as("feed")

    sessions.join(feeds,
      $"session.userId" === $"feed.userId" &&
        $"session.startedAt" <= $"feed.timestamp" &&
        ($"session.nextStartedAt" > $"feed.timestamp" || $"session.nextStartedAt".isNull),
      "left")
      .groupBy("date")
      .agg(
        countDistinct("session.userId", "sessionId").as("sessionCount"),
        avg("position").as("avgViewingDepth"),
        avg("durationMs").as("avgSessionDuration")
      )
      .show(100, false)
  }

  def countFeedsByUser(session: SparkSession): Unit = {
    val users = session.table(FeedsTable).select("userId").distinct().limit(100)
    val feeds = session.table(FeedsTable).select("userId", "durationMs")

    users.join(feeds, Seq("userId"), "left")
      .groupBy("userId")
      .count()
      .show(100, false)
  }


}