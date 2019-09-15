package io.github.antivanov.athena.query

import software.amazon.awssdk.services.athena.model.{GetQueryExecutionRequest, GetQueryResultsRequest}

case class QueryExecution(executionId: String) {

  def constructGetQueryExecutionRequest(): GetQueryExecutionRequest =
    GetQueryExecutionRequest.builder.queryExecutionId(executionId).build

  def constructGetQueryResults(): GetQueryResultsRequest =
    GetQueryResultsRequest.builder.queryExecutionId(executionId).build
}
