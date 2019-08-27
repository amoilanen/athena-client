package io.github.antivanov.athena.query

import scala.jdk.CollectionConverters._
import org.scalatest.{FreeSpec, Matchers}
import software.amazon.awssdk.services.athena.model.{Datum, Row}
import RowReader._

class RowReaderSpec extends FreeSpec with Matchers {

  def row(values: String*): Row = {
    val data = values.map(Datum.builder.varCharValue(_).build)
    Row.builder().data(data.asJava).build
  }

  "RowReader" - {

    "str" - {
      "correct index" in {
        str(0).readRow(row("abc")) shouldEqual "abc"
      }
      "incorrect index" in {
        val exception = intercept[Exception] {
          str(2).readRow(row("abc"))
        }
        exception shouldNot be(null)
      }
    }

    "int" - {
      "correct index" in {
        int(0).readRow(row("123")) shouldEqual 123
      }
      "incorrect index" in {
        val exception = intercept[Exception] {
          int(2).readRow(row("123"))
        }
        exception shouldNot be(null)
      }
      "not an int" in {
        val exception = intercept[Exception] {
          int(0).readRow(row("abc"))
        }
        exception shouldNot be(null)
      }
    }

    "list" - {
      "correct index" in {
        list[Int](int(0)).readRow(row("[1,2,3]")) shouldEqual List(1, 2, 3)
      }
    }
  }
}
