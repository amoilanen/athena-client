package io.github.antivanov.athena.query

import org.scalatest.{FreeSpec, Matchers}

class QueryExecutionSpec extends FreeSpec with Matchers {

  "QueryExecution" - {

    val query = "query"
    val executionId = "executionId"
    val execution = QueryExecution(query, executionId)

    "constructGetQueryExecutionRequest" in {
      execution.constructGetQueryExecutionRequest().queryExecutionId() shouldEqual executionId
    }

    "constructGetQueryResults" in {
      execution.constructGetQueryResults().queryExecutionId() shouldEqual executionId
    }
  }
}
