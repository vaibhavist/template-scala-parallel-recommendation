package com.bsb

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import grizzled.slf4j.Logger
import org.joda.time.{Days, DateTime}

case class DataSourceEvalParams(kFold: Int, queryNum: Int)

case class DataSourceParams(
  appName: String,
  evalParams: Option[DataSourceEvalParams]) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]

  def getRatingsOld(sc: SparkContext): RDD[Rating] = {

    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("rate", "buy")), // read "rate" and "buy" event
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)

    val ratingsRDD: RDD[Rating] = eventsRDD.map { event =>
      val rating = try {
        val ratingValue: Double = event.event match {
          case "rate" => event.properties.get[Double]("rating")
          case "buy" => 4.0 // map buy event to rating value of 4
          case _ => throw new Exception(s"Unexpected event ${event} is read.")
        }
        // entityId and targetEntityId is String
        Rating(event.entityId,
          event.targetEntityId.get,
          ratingValue)
      } catch {
        case e: Exception => {
          logger.error(s"Cannot convert ${event} to Rating. Exception: ${e}.")
          throw e
        }
      }
      rating
    }.cache()

    ratingsRDD
  }

  def sum(x: Event): Double = {
    val dateTime = new DateTime()
    var ratingValue: Double = 0
    var userId = ""
    var itemId = ""
      userId = x.entityId
      itemId = x.targetEntityId.get
      ratingValue = (
        x.event match {
          case "deleted" => ratingValue - (9.0 / (1 + Days.daysBetween(x.eventTime.toLocalDate(), dateTime.toLocalDate()).getDays()))
          case "removed" => ratingValue - (5.0 / (1 + Days.daysBetween(x.eventTime.toLocalDate(), dateTime.toLocalDate()).getDays()))
          case "unliked" => ratingValue - (6.0 / (1 + Days.daysBetween(x.eventTime.toLocalDate(), dateTime.toLocalDate()).getDays()))
          case "played" => ratingValue + (2.0 / (1 + Days.daysBetween(x.eventTime.toLocalDate(), dateTime.toLocalDate()).getDays()))
          case "played_long" => ratingValue + (3.0 / (1 + Days.daysBetween(x.eventTime.toLocalDate(), dateTime.toLocalDate()).getDays()))
          case "completed" => ratingValue + (4.0 / (1 + Days.daysBetween(x.eventTime.toLocalDate(), dateTime.toLocalDate()).getDays()))
          case "shared" => ratingValue + (6.0 / (1 + Days.daysBetween(x.eventTime.toLocalDate(), dateTime.toLocalDate()).getDays()))
          case "rented" => ratingValue + (8.0 / (1 + Days.daysBetween(x.eventTime.toLocalDate(), dateTime.toLocalDate()).getDays()))
          case "liked" => ratingValue + (5.0 / (1 + Days.daysBetween(x.eventTime.toLocalDate(), dateTime.toLocalDate()).getDays()))
          case "downloaded" => ratingValue + (7.0 / (1 + Days.daysBetween(x.eventTime.toLocalDate(), dateTime.toLocalDate()).getDays()))
          case "purchased" => ratingValue + (9.0 / (1 + Days.daysBetween(x.eventTime.toLocalDate(), dateTime.toLocalDate()).getDays()))
          case _ => throw new Exception(s"Unexpected event ${x} is read.")
        })
    logger.error(s"user: " + userId + ", item: " + itemId + ", event rating: " + ratingValue)
    ratingValue
  }

  def getRatings(sc: SparkContext): RDD[Rating] = {
    val dt = new DateTime()
    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("deleted", "removed", "unliked", "played", "played_long", "completed", "shared", "rented", "liked", "downloaded", "purchased")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item"))
      //startTime = Some(dt.minusDays(9)),
      //untilTime = Some(dt)
    )(sc)

    logger.error(s"NUMBER OF LINES : " + eventsRDD.count())
    val b = eventsRDD.map{
      case(event) =>
        (event.entityId, event.targetEntityId, event.eventTime, event.event) -> event
    }.reduceByKey((x,y) => x)

    val a = b.map{ case(event) =>
        val userId = event._1._1
        val itemId = event._1._2.get
        val rating = sum(event._2) // change
        val op = ((userId, itemId) -> rating)
        op
    }.reduceByKey((x, y) => x + y, 16)

    val ratingsRDD: RDD[Rating] = a.map{ b =>
      Rating(b._1._1, b._1._2, b._2)
    }.cache()

    ratingsRDD
  }

  override
  def readTraining(sc: SparkContext): TrainingData = {
    new TrainingData(getRatings(sc))
  }

  override
  def readEval(sc: SparkContext)
  : Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {
    require(!dsp.evalParams.isEmpty, "Must specify evalParams")
    val evalParams = dsp.evalParams.get

    val kFold = evalParams.kFold
    val ratings: RDD[(Rating, Long)] = getRatings(sc).zipWithUniqueId
    ratings.cache

    (0 until kFold).map { idx => {
      val trainingRatings = ratings.filter(_._2 % kFold != idx).map(_._1)
      val testingRatings = ratings.filter(_._2 % kFold == idx).map(_._1)

      val testingUsers: RDD[(String, Iterable[Rating])] = testingRatings.groupBy(_.user)

      (new TrainingData(trainingRatings),
        new EmptyEvaluationInfo(),
        testingUsers.map {
          case (user, ratings) => (Query(user, evalParams.queryNum), ActualResult(ratings.toArray))
        }
      )
    }}
  }
}

case class Rating(
  user: String,
  item: String,
  rating: Double
)

class TrainingData(
  val ratings: RDD[Rating]
) extends Serializable {
  override def toString = {
    s"ratings: [${ratings.count()}] (${ratings.take(2).toList}...)"
  }
}