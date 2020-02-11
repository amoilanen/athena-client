package io.github.antivanov.athena.query

import org.scalatest.{FreeSpec, Matchers}

class QueryExecutionSpec extends FreeSpec with Matchers {

  "QueryExecution" - {

    val executionId = "executionId"
    val execution = QueryExecution(executionId)

    "constructGetQueryExecutionRequest" in {
      execution.constructGetQueryExecutionRequest().queryExecutionId() shouldEqual executionId
    }

    "constructGetQueryResults" in {
      execution.constructGetQueryResults().queryExecutionId() shouldEqual executionId
    }
  }
}
