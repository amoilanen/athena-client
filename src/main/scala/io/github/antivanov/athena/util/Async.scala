package io.github.antivanov.athena.util

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.concurrent.{ExecutionContext, Future, Promise}

object Async {

  def schedule[T](delayMs: Int = 0)(block: () => Future[T])(implicit ec: ExecutionContext, scheduler: ScheduledExecutorService): Future[T] = {
    val promise = Promise[T]
    scheduler.schedule(new Runnable {
      override def run(): Unit = {
        block().map { value =>
          promise.success(value)
        }
      }
    }, delayMs, TimeUnit.MILLISECONDS)
    promise.future
  }

  def
}
