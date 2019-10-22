package co.ogury.test

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

import scala.util.{Failure, Success, Try}

trait BaseSpec extends FlatSpec with Matchers with GivenWhenThen {

  def withSparkSession[T](code: SparkSession => T): T = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    Try {
      code(session)
    } match {
      case Success(result) =>
        session.stop()
        result
      case Failure(exception) =>
        session.stop()
        throw exception
    }
  }

}
