package io.github.antivanov.athena.query

import org.scalatest.{FreeSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.athena.model.{Datum, Row}
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.{any}

import scala.jdk.CollectionConverters._

class QueryResultsSpec extends FreeSpec with MockitoSugar with Matchers {

  "QueryResults" - {

    "parse" - {

      trait TestCase {
        implicit val reader: RowReader[String] = mock[RowReader[String]]

        val range = (0 to 3)
        val rows = range.map(idx => {
          val datum = Datum.builder.varCharValue(idx.toString()).build()
          Row.builder.data(List(datum).asJava).build()
        })
        val values = range.map(idx => f"value$idx")

        range.foreach(idx =>
          when(reader.readRow(rows(idx))).thenReturn(values(idx))
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

      "should return left if RowReader throws an exception" in new TestCase {
        val error = new RuntimeException("Could not parse the value")
        when(reader.readRow(any())).thenThrow(error)

        QueryResults(rows).parse() shouldEqual Left(ParsingError(error))
      }
    }
  }
}
