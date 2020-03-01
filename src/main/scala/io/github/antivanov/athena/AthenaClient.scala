package io.github.antivanov.athena

import io.github.antivanov.athena.error.{AthenaClientError, QueryCancelledError, QueryFailedError, QueryResultsRetrievalError}
import io.github.antivanov.athena.query.{Query, QueryExecution, QueryResults, RowReader}
import software.amazon.awssdk.services.athena.model.QueryExecutionState.{CANCELLED, FAILED, QUEUED, RUNNING}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.athena
import software.amazon.awssdk.services.athena.{AthenaClient => JavaAthenaClient}
import software.amazon.awssdk.services.athena.model.Row
import util.Async.checkAtIntervalUntilReady

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class AthenaClient(configuration: AthenaConfiguration)(implicit context: ExecutionContext) {

  private val client: athena.AthenaClient = JavaAthenaClient.builder
    .region(configuration.region)
    .credentialsProvider(DefaultCredentialsProvider.create).build

  def executeUpdate(statement: String): Future[Either[Throwable, Unit]] = {

    object UnitReader extends RowReader[Unit] {

      def readRow(row: Row): Unit = {}
    }

    implicit val unitReader: RowReader[Unit] = UnitReader

    executeQuery(statement).map(_.map(_ => ()))
  }

  def executeQuery[T: RowReader](query: String): Future[Either[AthenaClientError, Seq[T]]] = {
    val queryExecution = submitQuery(query)
    getQueryResults[T](queryExecution).map(_.flatMap(_.parse))
  }

  private def submitQuery(query: String): QueryExecution = {
    val queryExecutionRequest = Query(query).constructStartQueryExecutionRequest(configuration)
    val queryExecutionId = client.startQueryExecution(queryExecutionRequest).queryExecutionId

    QueryExecution(query, queryExecutionId)
  }

  private def getQueryResults[T: RowReader](queryExecution: QueryExecution): Future[Either[AthenaClientError, QueryResults[T]]] = {
    checkAtIntervalUntilReady { () =>
      checkQueryExecutionResults(queryExecution)
    } (configuration.queryExecutionCheckIntervalMs, configuration.queryTimeoutMs)
      .map(Right(_)) recover {
        case error: AthenaClientError =>
          Left(error)
        case NonFatal(error) =>
          Left(QueryResultsRetrievalError(queryExecution, error))
    }
  }

  private def checkQueryExecutionResults[T: RowReader](queryExecution: QueryExecution): Option[QueryResults[T]] = {
    val queryExecutionResponse = client.getQueryExecution(queryExecution.constructGetQueryExecutionRequest)
    val queryState = queryExecutionResponse.queryExecution.status.state

    if (queryState == FAILED) {
      throw QueryFailedError(queryExecution)
    } else if (queryState == CANCELLED) {
      throw QueryCancelledError(queryExecution)
    } else if (queryState == RUNNING || queryState == QUEUED) {
      None
    } else {
      val queryResults = client.getQueryResults(queryExecution.constructGetQueryResults)
      val rows: Seq[Row] = queryResults.resultSet.rows.asScala.toList

      Some(QueryResults[T](rows))
    }
  }
}
