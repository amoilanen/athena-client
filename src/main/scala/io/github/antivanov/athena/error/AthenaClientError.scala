package io.github.antivanov.athena.error

import io.github.antivanov.athena.query.QueryExecution
import software.amazon.awssdk.services.athena.model.Row

sealed abstract class AthenaClientError(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

case class QueryCancelledError(queryExecution: QueryExecution)
  extends AthenaClientError(s"Query cancelled ${queryExecution.toString}")

case class QueryFailedError(queryExecution: QueryExecution)
  extends AthenaClientError(s"Query failed ${queryExecution.toString}")

case class QueryResultsRetrievalError(queryExecution: QueryExecution, error: Throwable)
  extends AthenaClientError(s"Failed to retrieve query results ${queryExecution.toString}", error)

case class QueryResultsParsingError(rows: Seq[Row], error: Throwable)
  extends AthenaClientError(s"Failed to parse rows ${rows.toString}", error)

