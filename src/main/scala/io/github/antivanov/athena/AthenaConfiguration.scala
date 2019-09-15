package io.github.antivanov.athena

import software.amazon.awssdk.regions.Region

case class AthenaConfiguration(databaseName: String, outputBucketName: String, region: Region)
