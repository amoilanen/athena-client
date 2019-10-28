package io.github.antivanov.athena

import io.github.antivanov.athena.query.RowReader
import io.github.antivanov.athena.query.RowReader._
import io.github.antivanov.athena.util.S3Client
import org.scalatest.{BeforeAndAfterAll, FreeSpec, Matchers}
import software.amazon.awssdk.regions.Region

import scala.concurrent.Await
import scala.io.Source

class AthenaClientItSpec extends FreeSpec with QuerySlownessTolerance with Matchers with BeforeAndAfterAll {

  implicit val executionContext = scala.concurrent.ExecutionContext.Implicits.global

  val athenaDatabase = "athenaittests"
  val outputBucketName = "athena-it-tests-26449690-45c1-46e8-aff9-235e8cced1b2"
  val inputBucketName = "athena-it-tests-input-d90b3d7d-35a1-49c9-9bbe-47222ea07d80"

  val region = Region.EU_CENTRAL_1
  val athenaClient = new AthenaClient(AthenaConfiguration(athenaDatabase, f"s3://$outputBucketName", region))
  val s3Client = S3Client(region)

  val tableData =
    """
      |city,population
      |Helsinki,650033
      |Espoo,283944
      |Tampere,235487
      |Vantaa,228166
      |Oulu,203623
      |Turku,191664
      |Jyväskylä,141414
      |Lahti,120002
      |Kuopio,118727
      |Pori,84391
      |Joensuu,76577
      |Lappeenranta,72705
      |Vaasa,67596
      |Hämeenlinna,67558
      |Seinäjoki,63296
      |Rovaniemi,62963
      |Mikkeli,53843
      |Kotka,52930
      |Salo,52332
      |""".stripMargin

  def setUpBuckets(): Unit = {
    s3Client.ensureExists(inputBucketName)
    s3Client.putObject(inputBucketName, "cities.csv", tableData)
    s3Client.ensureExists(outputBucketName)
  }

  def setUpAthena(): Unit = {
    val rowRegex = "^(\\\\S+),(\\\\S+)$"
    val createTableSql =
      f"""
         |create external table if not exists athenaittests.cities(
         |  city string,
         |  population int
         |) row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
         |with serdeproperties (
         |"input.regex" = "$rowRegex"
         |) location "s3://$inputBucketName/";
         |""".stripMargin

    Await.result(athenaClient.executeUpdate(f"create database $athenaDatabase;"), queryTimeOut)
    Await.result(athenaClient.executeUpdate(createTableSql), queryTimeOut)
  }

  def tearDownBuckets(): Unit = {
    s3Client.deleteBucket(outputBucketName)
    s3Client.deleteBucket(inputBucketName)
  }

  def tearDownAthena(): Unit = {
    Await.result(athenaClient.executeUpdate(f"drop database if exists $athenaDatabase cascade;"), queryTimeOut)
  }

  override def beforeAll(): Unit = {
    setUpBuckets()
    setUpAthena()
  }

  override def afterAll(): Unit = {
    tearDownAthena()
    tearDownBuckets()
  }

  def readResource(fileName: String): String =
    Source.fromResource(fileName).getLines().toList.mkString("\n")

  "AthenaClient" - {

    "should execute query and get results" in {

      case class CityPopulation(city: String, population: Int)

      implicit val cityPopulationReader: RowReader[CityPopulation] =
        str(0) ~
        int(1) map {
          case city ~ population =>
            CityPopulation(city, population)
        }

      val query = "select * from cities order by population desc limit 5;"

      val expected = Right(
        List(
          CityPopulation("Helsinki", 650033),
          CityPopulation("Espoo", 283944),
          CityPopulation("Tampere",235487),
          CityPopulation("Vantaa",228166),
          CityPopulation("Oulu",203623)
        )
      )
      Await.result(athenaClient.executeQuery(query), queryTimeOut) shouldEqual expected
    }
  }
}
