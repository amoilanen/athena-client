package io.github.antivanov.athena

import io.github.antivanov.athena.query.RowReader
import io.github.antivanov.athena.query.RowReader._
import software.amazon.awssdk.regions.Region
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.language.postfixOps

object Main extends App {

  implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global

  val query = "select * from cities order by population desc limit 5;"

  case class CityPopulation(city: String, population: Int)

  implicit val cityPopulationReader: RowReader[CityPopulation] =
    str(0) ~
    int(1) map {
      case city ~ population =>
        CityPopulation(city, population)
    }

  val athenaOutputBucket = "s3://anton.al.ivanov-athena-output-bucket"
  val athenaDatabase = "athenatests"

  val athenaClient = new AthenaClient(AthenaConfiguration(athenaDatabase, athenaOutputBucket, Region.EU_CENTRAL_1))
  val queryResults: Future[Either[Throwable, Seq[CityPopulation]]] = athenaClient.executeQuery(query)

  val results: Either[Throwable, Seq[CityPopulation]] = Await.result(queryResults, 10 seconds)

  println(results)
}
