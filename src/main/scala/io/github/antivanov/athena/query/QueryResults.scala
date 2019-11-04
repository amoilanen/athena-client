package io.github.antivanov.athena.query

import software.amazon.awssdk.services.athena.model.{ColumnInfo, Row}

import scala.util.Try

case class QueryResultsParsingError(error: Throwable) extends RuntimeException(error)

case class QueryResults[T: RowReader](rows: Seq[Row]) {
  def parse(): Either[QueryResultsParsingError, Seq[T]] = {
    val rowReader = implicitly[RowReader[T]]
    Try(rows.drop(1).map(rowReader.readRow(_)))
      .toEither.left.map(error => QueryResultsParsingError(error))
  }
}