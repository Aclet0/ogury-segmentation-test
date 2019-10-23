package co.ogury.segmentation

import java.sql.Date
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.UUID

import co.ogury.segmentation.ActivitySegment.ActivitySegment
import co.ogury.test.BaseSpec
import org.apache.spark.sql.Row

class SegmentationJobSpec extends BaseSpec {

  private val Id = "customerId"
  private val defaultCustomerId = "customerId"
  private val otherCustomerId = "otherCustomerId"
  private val otherTransactions = Seq(
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 1)),
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 5)),
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 6)),
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 10)),
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 15)),
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 16)),
    genTransaction(otherCustomerId, LocalDate.of(2018, 1, 20))
  )

  private val startDate = Date.valueOf(LocalDate.of(2018, 1, 6))
  private val endDate = Date.valueOf(LocalDate.of(2018, 1, 15))

  "ActivitySegmentation" should "be 'undefined' for customers who have never bought" in {
    withSparkSession { session =>
      Given("A customer without transactions")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = otherTransactions.toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("Check segmentation ")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.UNDEFINED)
    }
  }

  it should "be 'undefined' for customers who have first purchase after period" in {
    withSparkSession { session =>
      Given("A customer with first purchase after period")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 17))
        )
          ++ otherTransactions
      ).toDS()

      When("computing segmentation")
      val segmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("check segmentation")
      defaultCustomerShouldBeInSegment(segmentationDF.collect(), ActivitySegment.UNDEFINED)

    }
  }

  it should "be 'new' if the first purchase is during period" in {
    withSparkSession { session =>
      Given("A customer with first purchase during period")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 10))
        )
          ++ otherTransactions
        ).toDS()

      When("computing segmentation")
      val segmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("check segmentation")
      defaultCustomerShouldBeInSegment(segmentationDF.collect(), ActivitySegment.NEW)

    }  }

  it should "be 'new' if the first purchase is during period with multiple purchases" in {
    withSparkSession { session =>
      Given("Multiple transaction during period")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 10)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 2, 15))
        )
          ++ otherTransactions
      ).toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("Check segmentation ")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.NEW)
    }
  }

  it should "be 'new' if the first purchase is during period and with multiple purchases during and after period" in {
    withSparkSession { session =>
      Given("Multiple transaction during period and multiple after")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 10)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 2, 15)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 2, 19)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 2, 20))
        )
          ++ otherTransactions
        ).toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("Check segmentation ")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.NEW)
    }

  }

  it should "be 'active' if there is a first-purchase is before P and a non-first purchase during P" in {
    withSparkSession { session =>
      Given("First purchase is before P and another purchase during P")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 4)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 13))
        )
          ++ otherTransactions
        ).toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("Check segmentation ")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.ACTIVE)
    }
  }

  it should "be 'Inactive' if the first purchase is before period but no purchase during p" in {
    withSparkSession { session =>
      Given("First purchase before P and no other purchase in P")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 4))
        )
          ++ otherTransactions
        ).toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("Check segmentation ")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.INACTIVE)
    }
  }

  it should "be 'Inactive' if the first purchase is before period and purchases after but no purchase during p" in {
    withSparkSession { session =>
      Given("First purchase before P and other purchases present but none in P")
      import session.implicits._
      val customers = Seq(defaultCustomerId, otherCustomerId).toDF(Id).as[String]
      val transactions = (
        Seq(
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 4)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 5)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 17)),
          genTransaction(defaultCustomerId, LocalDate.of(2018, 1, 18))
        )
          ++ otherTransactions
        ).toDS()

      When("computing segmentation")
      val SegmentationDF = SegmentationJob.computeSegmentation(customers, transactions, startDate, endDate)

      Then("Check segmentation ")
      defaultCustomerShouldBeInSegment(SegmentationDF.collect(), ActivitySegment.INACTIVE)
    }
  }

  private def genTransaction(customerId: String, date: LocalDate): Transaction =
    Transaction(
      UUID.randomUUID().toString,
      customerId,
      "2",
      date.atStartOfDay(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
      10,
      1
    )

  private def defaultCustomerShouldBeInSegment(segmentations: Seq[Row], segment: ActivitySegment): Unit = {
    Then("the segmentation processed 2 customers")
    segmentations should not be empty
    segmentations.length shouldBe 2
    And(s"the customer is classified as $segment'")
    segmentations.find(_.getString(0) == defaultCustomerId) match {
      case None      => fail("customer have not been found")
      case Some(row) => row.getString(1) shouldBe segment.toString
    }
  }




}
