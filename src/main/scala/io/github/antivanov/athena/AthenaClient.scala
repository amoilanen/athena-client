package io.github.antivanov.athena

import java.util.{Timer, TimerTask}
import java.util.concurrent.{Executors, ScheduledExecutorService}

import io.github.antivanov.athena.query.{Query, QueryExecution, QueryResults, RowReader}
import software.amazon.awssdk.services.athena.model.QueryExecutionState.{CANCELLED, FAILED, QUEUED, RUNNING}
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.services.athena
import software.amazon.awssdk.services.athena.AthenaClient
import software.amazon.awssdk.services.athena.model.Row

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}

class AthenaClient(configuration: AthenaConfiguration)(implicit context: ExecutionContext) {

  private val client: athena.AthenaClient = AthenaClient.builder
    .region(configuration.region)
    .credentialsProvider(DefaultCredentialsProvider.create).build

  def executeQuery[T: RowReader](query: String): Future[Either[Throwable, Seq[T]]] = {
    val queryExecution = submitQuery(query)
    getQueryResults[T](queryExecution).map(_.map(_.parse))
  }

  private def submitQuery(query: String): QueryExecution = {
    val queryExecutionRequest = Query(query).constructStartQueryExecutionRequest(configuration)
    val queryExecutionId = client.startQueryExecution(queryExecutionRequest).queryExecutionId

    QueryExecution(queryExecutionId)
  }

  private def getQueryResults[T: RowReader](queryExecution: QueryExecution): Future[Either[Throwable, QueryResults[T]]] = {
    val promise = Promise[Either[Throwable, QueryResults[T]]]
    val timer = new Timer()
    //TODO: Timeout if query executes for too long
    //TODO: Re-factor a common pattern from this asynchronous code?
    val checkQueryExecutionTask = new TimerTask {
      def run(): Unit = {
        val checkResult = checkQueryExecutionResults(queryExecution)
        checkResult match {
          case Left(error) => {
            promise.success(Left(error))
          }
          case Right(QueryResults(Some(rows))) => {
            promise.success(Right(QueryResults(Some(rows))))
          }
          case Right(QueryResults(None)) => {
          }
        }
      }
    }
    timer.schedule(checkQueryExecutionTask, 0L, configuration.queryExecutionCheckIntervalMs)
    val future = promise.future
    future.onComplete {_ =>
      checkQueryExecutionTask.cancel()
    }
    future
  }

  private def checkQueryExecutionResults[T: RowReader](queryExecution: QueryExecution): Either[Throwable, QueryResults[T]] = {
    val queryExecutionResponse = client.getQueryExecution(queryExecution.constructGetQueryExecutionRequest)
    val queryState = queryExecutionResponse.queryExecution.status.state

    if (queryState == FAILED) {
      //TODO: Define custom errors for AthenaClient?
      Left(throw new RuntimeException("Query failed"))
    } else if (queryState == CANCELLED) {
      Left(new RuntimeException("Query was cancelled"))
    } else if (queryState == RUNNING || queryState == QUEUED) {
      Right(QueryResults(None))
    } else {
      val queryResults = client.getQueryResults(queryExecution.constructGetQueryResults)
      val rows: Seq[Row] = queryResults.resultSet.rows.asScala.toList

      Right(QueryResults[T](Option(rows)))
    }
  }
}
