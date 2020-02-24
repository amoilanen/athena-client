package io.github.antivanov.athena.query

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FreeSpec, Matchers}
import software.amazon.awssdk.services.athena.model.{Datum, Row}

import scala.jdk.CollectionConverters._

class QueryResultsSpec extends FreeSpec with MockFactory with Matchers {

  "QueryResults" - {

    val range = (0 to 3)
    val rows = range.map(idx => {
      val datum = Datum.builder.varCharValue(idx.toString()).build()
      Row.builder.data(List(datum).asJava).build()
    })
    val values = range.map(idx => f"value$idx")

    "parse" - {

      trait TestCase {
        implicit val reader: RowReader[String] = stub[RowReader[String]]

        range.foreach(idx =>
          (reader.readRow _).when(rows(idx)).returns(values(idx))
        )
      }

      "should parse non-empty results" in new TestCase {
        QueryResults(rows).parse() shouldEqual Right(values.drop(1))
      }

      "should parse results containing one row" in new TestCase {
        QueryResults(rows.take(2)).parse() shouldEqual Right(Seq(values(1)))
      }

      "should parse results containing only header" in new TestCase {
        QueryResults(rows.take(1)).parse() shouldEqual Right(Seq())
      }

      "should parse results containing no header and no rows" in new TestCase {
        QueryResults(rows.take(0)).parse() shouldEqual Right(Seq())
      }

      "should return left if RowReader throws an exception" in {
        implicit val reader: RowReader[String] = stub[RowReader[String]]

        val error = new RuntimeException("Could not parse the value")
        (reader.readRow _).when(*).throws(error)

        QueryResults(rows).parse() shouldEqual Left(QueryResultsParsingError(error))
      }
    }
  }
}
