package io.github.antivanov.athena

import io.github.antivanov.athena.query.RowReader
import io.github.antivanov.athena.query.RowReader._
import software.amazon.awssdk.regions.Region

object Main extends App {

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
  val queryResults: Seq[CityPopulation] = athenaClient.executeQuery(query)

  println(queryResults)
}
