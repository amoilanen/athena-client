package io.github.antivanov.athena.query

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import software.amazon.awssdk.services.athena.model.Row

import scala.reflect.ClassTag
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

  //TODO: Use ISO8061 or define/use some constants for commonly used formats
  val DefaultFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

  def timestamp(columnIndex: Int, format: String = DefaultFormat): RowReader[Timestamp] = new ColumnRowReader[Timestamp](columnIndex,
    (value: String) => {
      val date = new SimpleDateFormat(format).parse(value)
      new Timestamp(date.getTime)
    }
  )

  def date(columnIndex: Int, format: String = DefaultFormat): RowReader[Date] = new ColumnRowReader[Date](columnIndex,
    (value: String) => {
      new SimpleDateFormat(format).parse(value)
    }
  )

  def array[A: ClassTag](reader: ColumnRowReader[A]): RowReader[Array[A]] =
    (row: Row) => list[A](reader).readRow(row).toArray[A]

  def list[A](reader: ColumnRowReader[A]): ColumnRowReader[List[A]] = new ColumnRowReader[List[A]](reader.columnIndex,
    (value: String) => {
      val listValues: Seq[String] = value.split("[,\\[\\]]").toList.filter(_.length > 0)
      listValues.map { listValue =>
        reader.parser(listValue)
      }.toList
    }
  )

  def ofType[A](columnIndex: Int)(implicit parse: String => A) =
    new ColumnRowReader[A](columnIndex, parse)
}