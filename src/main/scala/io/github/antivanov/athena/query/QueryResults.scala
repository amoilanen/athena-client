package io.github.antivanov.athena.query

import software.amazon.awssdk.services.athena.model.{ColumnInfo, Row}

//TODO: Add total execution time to the results
case class QueryResults[T: RowReader](rows: Option[Seq[Row]]) {
  def parse(): Seq[T] = {
    val rowReader = implicitly[RowReader[T]]
    rows.getOrElse(Seq()).drop(1).map(rowReader.readRow(_))
  }
}