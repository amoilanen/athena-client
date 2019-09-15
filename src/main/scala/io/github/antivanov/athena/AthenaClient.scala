package io.github.antivanov.athena

import io.github.antivanov.athena.query.{Query, QueryExecution, QueryResults, RowReader}
import software.amazon.awssdk.services.athena.model.QueryExecutionState.{CANCELLED, FAILED, QUEUED, RUNNING}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.athena
import software.amazon.awssdk.services.athena.AthenaClient
import software.amazon.awssdk.services.athena.model.Row

import scala.jdk.CollectionConverters._

import scala.annotation.tailrec

class AthenaClient(athenaConfiguration: AthenaConfiguration) {

  private val client: athena.AthenaClient = AthenaClient.builder
    .region(athenaConfiguration.region)
    .credentialsProvider(DefaultCredentialsProvider.create).build

  def executeQuery[T: RowReader](query: String): Seq[T] = {
    val queryExecution = submitQuery(query)
    waitForQueryExecutionToComplete(queryExecution)
    getQueryResults[T](queryExecution).parse
  }

  private def submitQuery(query: String): QueryExecution = {
    val queryExecutionRequest = Query(query).constructStartQueryExecutionRequest(athenaConfiguration)
    val queryExecutionId = client.startQueryExecution(queryExecutionRequest).queryExecutionId

    QueryExecution(queryExecutionId)
  }

  @tailrec
  private def waitForQueryExecutionToComplete(queryExecution: QueryExecution): Unit = {
    //TODO: In asynchronous version schedule the check to be executed later
    Thread.sleep(1000)

    val queryExecutionResponse = client.getQueryExecution(queryExecution.constructGetQueryExecutionRequest)
    val queryState = queryExecutionResponse.queryExecution.status.state

    if (queryState == FAILED) {
      throw new RuntimeException("Query failed")
    } else if (queryState == CANCELLED) {
      throw new RuntimeException("Query was cancelled")
    } else if (queryState == RUNNING || queryState == QUEUED) {
      waitForQueryExecutionToComplete(queryExecution)
    }
  }

  private def getQueryResults[T: RowReader](queryExecution: QueryExecution): QueryResults[T] = {
    val queryResults = client.getQueryResults(queryExecution.constructGetQueryResults)
    val rows: Seq[Row] = queryResults.resultSet.rows.asScala.toList

    QueryResults[T](rows)
  }
}
