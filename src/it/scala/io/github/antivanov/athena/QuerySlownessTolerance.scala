package io.github.antivanov.athena

import scala.concurrent.duration._
import scala.language.postfixOps

trait QuerySlownessTolerance {

  val queryTimeOut = 5 seconds
}
