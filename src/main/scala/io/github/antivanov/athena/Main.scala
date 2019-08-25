package io.github.antivanov.athena

import io.github.antivanov.athena.query.{QueryResults, RowReader}
import io.github.antivanov.athena.query.RowReader._
import software.amazon.awssdk.auth.credentials.{DefaultCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.athena
import software.amazon.awssdk.services.athena.AthenaClient
import software.amazon.awssdk.services.athena.model._
import software.amazon.awssdk.services.athena.model.QueryExecutionState.{CANCELLED, FAILED, QUEUED, RUNNING}

import scala.jdk.CollectionConverters._
import scala.annotation.tailrec

// Based on https://docs.aws.amazon.com/athena/latest/ug/code-samples.html#start-query-execution
object Main extends App {

  object Config {
    val ATHENA_OUTPUT_BUCKET = "s3://anton.al.ivanov-athena-output-bucket"
    val ATHENA_DATABASE = "athenatests"
  }

  case class QueryExecutionId(value: String) {}

  def submitQuery(query: String): QueryExecutionId = {
    val resultConfiguration = ResultConfiguration.builder
      .outputLocation(Config.ATHENA_OUTPUT_BUCKET).build
    val queryContext = QueryExecutionContext.builder
      .database(Config.ATHENA_DATABASE).build
    val queryExecutionRequest = StartQueryExecutionRequest.builder
      .queryString(query).queryExecutionContext(queryContext).resultConfiguration(resultConfiguration).build
    val queryExecution = athenaClient.startQueryExecution(queryExecutionRequest)
    val queryExecutionId = athenaClient.startQueryExecution(queryExecutionRequest).queryExecutionId

    QueryExecutionId(queryExecutionId)
  }

  //TODO: Add support for timing out the query in case it takes too long?
  @tailrec
  def waitForQueryExecutionToComplete(queryExecutionId: QueryExecutionId): Unit = {
    //TODO: In asynchronous version schedule the check to be executed later
    Thread.sleep(1000)
    val getQueryExecutionRequest = GetQueryExecutionRequest.builder.queryExecutionId(queryExecutionId.value).build

    val queryExecutionResponse = athenaClient.getQueryExecution(getQueryExecutionRequest)
    val queryState = queryExecutionResponse.queryExecution.status.state

    if (queryState == FAILED) {
      throw new RuntimeException("Query failed")
    } else if (queryState == CANCELLED) {
      throw new RuntimeException("Query was cancelled")
    } else if (queryState == RUNNING || queryState == QUEUED) {
      waitForQueryExecutionToComplete(queryExecutionId)
    }
  }

  def getQueryResults[T: RowReader](queryExecutionId: QueryExecutionId): QueryResults[T] = {
    val getResultsRequest = GetQueryResultsRequest.builder.queryExecutionId(queryExecutionId.value).build

    val queryResults = athenaClient.getQueryResults(getResultsRequest)

    val columns: Seq[ColumnInfo] = queryResults.resultSet.resultSetMetadata.columnInfo.asScala.toList
    val rows: Seq[Row] = queryResults.resultSet.rows.asScala.toList

    QueryResults[T](columns, rows)
  }

  val query = "select * from cities order by population desc limit 5;"

  val athenaClient: athena.AthenaClient = AthenaClient.builder
    .region(Region.EU_CENTRAL_1)
    .credentialsProvider(DefaultCredentialsProvider.create).build

  case class CityPopulation(city: String, population: Int)

  implicit val cityPopulationReader: RowReader[CityPopulation] =
    str(0) ~
    int(1) map {
      case city ~ population =>
        CityPopulation(city, population)
    }

  val queryExecution = submitQuery(query)
  waitForQueryExecutionToComplete(queryExecution)
  val queryResults: Seq[CityPopulation] = getQueryResults[CityPopulation](queryExecution).parse

  println(queryResults)
}
