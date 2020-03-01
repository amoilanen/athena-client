package io.github.antivanov.athena.query

import scala.util.Try
import software.amazon.awssdk.services.athena.model.Row
import io.github.antivanov.athena.error.QueryResultsParsingError

case class QueryResults[T: RowReader](rows: Seq[Row]) {
  def parse(): Either[QueryResultsParsingError, Seq[T]] = {
    val rowReader = implicitly[RowReader[T]]
    Try(rows.drop(1).map(rowReader.readRow(_)))
      .toEither.left.map(error => QueryResultsParsingError(rows, error))
  }
}