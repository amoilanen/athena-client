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
        list(int(0)).readRow(row("[1,2,3]")) shouldEqual List(1, 2, 3)
      }

      "incorrect index" in {
        val exception = intercept[Exception] {
          list(int(3)).readRow(row("[1,2,3]"))
        }
        exception shouldNot be(null)
      }
    }

    "array" - {
      "correct index" in {
        array(int(0)).readRow(row("[1,2,3]")) shouldEqual Array(1, 2, 3)
      }

      "incorrect index" in {
        val exception = intercept[Exception] {
          array(int(3)).readRow(row("[1,2,3]"))
        }
        exception shouldNot be(null)
      }
    }

    "ofType" - {

      case class Foo(x: Int, y: Int)

      implicit def fooParser(value: String): Foo = {
        val Array(x, y) = value.split(',').map(_.toInt)
        Foo(x, y)
      }

      "correct index" in {
        ofType[Foo](0).readRow(row("1,2")) shouldEqual Foo(1, 2)
      }

      "incorrect index" in {
        val exception = intercept[Exception] {
          ofType[Foo](1).readRow(row("1,2"))
        }
        exception shouldNot be(null)
      }
    }
  }
}
