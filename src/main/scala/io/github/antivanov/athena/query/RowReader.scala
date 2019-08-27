package io.github.antivanov.athena.query

import java.sql.Timestamp
import java.util.Date
import scala.jdk.CollectionConverters._

import software.amazon.awssdk.services.athena.model.Row

import scala.util.Try

final case class RowParsingException(cause: Throwable) extends Exception(cause)

/*
 * Mechanism to combine the Readers together is inspired by Anorm https://github.com/playframework/anorm
 */
trait RowReader[A] {

  import RowReader._

  def readRowTry(row: Row): Either[RowParsingException, A] =
    Try(readRow(row)).toEither.left.map(RowParsingException(_))

  def readRow(row: Row): A

  def ~[B](otherRowReader: RowReader[B]): RowReader[A ~ B] = (row: Row) => {
    val readValue = readRow(row)
    val otherReadValue = otherRowReader.readRow(row)

    RowReader.~(readValue, otherReadValue)
  }

  def map[B](f: A => B): RowReader[B] = (row: Row) =>
    f(readRow(row))
}

case class ColumnRowReader[A](columnIndex: Int, parser: String => A) extends RowReader[A] {

  def getParser: String => A = parser

  final def readRow(row: Row): A = {
    parser(row.data().get(columnIndex).varCharValue())
  }
}

object RowReader {

  final case class ~[+A, +B](_1: A, _2: B)

  def str(columnIndex: Int): ColumnRowReader[String] = new ColumnRowReader[String](columnIndex,
    (value: String) => value
  )

  def int(columnIndex: Int): ColumnRowReader[Int] = new ColumnRowReader[Int](columnIndex,
    (value: String) => value.toInt
  )

  def double(columnIndex: Int): ColumnRowReader[Double] = new ColumnRowReader[Double](columnIndex,
    (value: String) => value.toDouble
  )

  def bigDecimal(columnIndex: Int): ColumnRowReader[BigDecimal] = new ColumnRowReader[BigDecimal](columnIndex,
    (value: String) => BigDecimal(value)
  )

  def timestamp(columnIndex: Int): RowReader[Timestamp] =
    (row: Row) => ???

  def date(columnIndex: Int): RowReader[Date] =
    (row: Row) => ???

  def array[A](reader: ColumnRowReader[A]): RowReader[Array[A]] =
    (row: Row) => ???

  def list[A](reader: ColumnRowReader[A]): ColumnRowReader[List[A]] = new ColumnRowReader[List[A]](reader.columnIndex,
    (value: String) => {
      val listValues: Seq[String] = value.split("[,\\[\\]]").toList.filter(_.length > 0)
      listValues.map { listValue =>
        reader.parser(listValue)
      }.toList
    }
  )

  //TODO: Custom ColumnRowReader
}