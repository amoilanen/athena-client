package io.github.antivanov.athena.query

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

object RowReader {

  final case class ~[+A, +B](_1: A, _2: B)

  //TODO: Add more different readers to support all the Athena types and also common cases such as storing an array
  def str(columnIndex: Int): RowReader[String] =
    (row: Row) => row.data().get(columnIndex).varCharValue()

  def int(columnIndex: Int): RowReader[Int] =
    (row: Row) => row.data().get(columnIndex).varCharValue().toInt
}