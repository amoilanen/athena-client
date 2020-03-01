package io.github.antivanov.athena.query

import software.amazon.awssdk.services.athena.model.{GetQueryExecutionRequest, GetQueryResultsRequest}

case class QueryExecution(query: String, executionId: String) {

  def constructGetQueryExecutionRequest(): GetQueryExecutionRequest =
    GetQueryExecutionRequest.builder.queryExecutionId(executionId).build

  def constructGetQueryResults(): GetQueryResultsRequest =
    GetQueryResultsRequest.builder.queryExecutionId(executionId).build

  override def toString: String =
    s"QueryExecution(query='${query}', id=${executionId})"
}
