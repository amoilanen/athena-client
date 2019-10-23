package io.github.antivanov.athena

import org.scalatest.{FreeSpec, Matchers}
import software.amazon.awssdk.services.s3.S3ClientBuilder

class AthenaClientSpec extends FreeSpec with Matchers {

  val s3Client: AmazonS3 = S3ClientBuilder.standard.withCredentials(new ProfileCredentialsProvider).withRegion(clientRegion).build


  val s3Client = AmazonS3ClientBuilder.standard()
    .withCredentials(new ProfileCredentialsProvider())
    .withRegion(clientRegion)
    .build();

  "AthenaClient" - {

    "should be able to execute a query" - {

    }
  }
}
