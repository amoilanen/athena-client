package io.github.antivanov.athena.query

import io.github.antivanov.athena.AthenaConfiguration
import software.amazon.awssdk.services.athena.model.{QueryExecutionContext, ResultConfiguration, StartQueryExecutionRequest}

case class Query(queryString: String) {

  def constructStartQueryExecutionRequest(athenaConfiguration: AthenaConfiguration): StartQueryExecutionRequest = {
    val resultConfiguration = ResultConfiguration.builder
      .outputLocation(athenaConfiguration.outputBucketName).build
    val queryContext = QueryExecutionContext.builder
      .database(athenaConfiguration.databaseName).build
    StartQueryExecutionRequest.builder
      .queryString(queryString).queryExecutionContext(queryContext).resultConfiguration(resultConfiguration).build
  }
}
