package io.github.antivanov.athena.query

import software.amazon.awssdk.services.athena.model.{ColumnInfo, Row}

import scala.util.Try

case class ParsingError(error: Throwable) extends RuntimeException(error)

//TODO: Add total execution time to the results
case class QueryResults[T: RowReader](rows: Seq[Row]) {
  def parse(): Either[ParsingError, Seq[T]] = {
    val rowReader = implicitly[RowReader[T]]
    Try(rows.drop(1).map(rowReader.readRow(_)))
      .toEither.left.map(error => ParsingError(error))
  }
}