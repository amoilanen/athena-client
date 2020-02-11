package io.github.antivanov.athena.query

import io.github.antivanov.athena.AthenaConfiguration
import org.scalatest.{FreeSpec, Matchers}
import software.amazon.awssdk.regions.Region

class QuerySpec extends FreeSpec with Matchers {

  "Query" - {

    val databaseName = "database"
    val outputBucketName = "outputBucket"
    val region = Region.EU_NORTH_1
    val queryExecutionCheckIntervalMs = 1
    val queryTimeoutMs = 2
    val configuration = AthenaConfiguration(
      databaseName,
      outputBucketName,
      region,
      queryExecutionCheckIntervalMs,
      queryTimeoutMs
    )
    val queryString = "query"
    val query = Query(queryString)

    "constructStartQueryExecutionRequest" in {
      val request = query.constructStartQueryExecutionRequest(configuration)

      request.queryString shouldEqual queryString
      request.resultConfiguration.outputLocation shouldEqual outputBucketName
      request.queryExecutionContext.database shouldEqual databaseName
    }
  }
}
