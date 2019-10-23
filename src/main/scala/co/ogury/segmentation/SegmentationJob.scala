package co.ogury.segmentation

import java.sql.Date
import java.time.LocalDate

import util.control.Breaks._

import co.ogury.segmentation.ActivitySegment.ActivitySegment

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Random

object SegmentationJob {

  def main(args: Array[String]): Unit = {

    val dataPath = args(0)
    val outputFile = args(1)
    val endDate = args(2)
    val segmentationPeriod = args(3).toInt

    val session = createSparkSession()

    run(session, dataPath, outputFile, LocalDate.parse(endDate), segmentationPeriod.toInt)

    //val idUdf = udf { Random.alphanumeric.take(7).map(_.toLower).mkString }

    session.stop()
  }

  def run(session: SparkSession, dataPath: String, outputFile: String, endDate: LocalDate, segmentationPeriod: Int): Unit = {
    // load
    val transactions = loadTransactions(session, dataPath)
    val customers = loadCustomers(session, dataPath)

    // compute
    val startDate = endDate.plusDays(segmentationPeriod)
    val computed_df = computeSegmentation(customers, transactions, Date.valueOf(startDate), Date.valueOf(endDate))

    // save
    saveSegmentation(computed_df, outputFile)
  }


  private def loadTransactions(session: SparkSession, dataPath: String): Dataset[Transaction] = {
    import session.implicits._

    val transactionDF = session.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(dataPath + "transactions/transactions.csv")

    return transactionDF.as[Transaction]
  }

  private def loadCustomers(session: SparkSession, dataPath: String): Dataset[String] = {
    import session.implicits._

    val customerDF = session.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(dataPath + "customers/customerIds.csv")

    return customerDF.as[String]
  }



  private def getActivitySegment(transaction_dates: Seq[Date], startDate:Date, endDate: Date): String = {
    if(transaction_dates != null){
      // important to sort date before iterating on it
      val sorted_dates = transaction_dates.sortBy(_.getTime)
      var has_before_start_date_trans: Boolean = false
      var has_during_period_trans: Boolean = false
      breakable{
        for (date <- sorted_dates){

          if(date.after(endDate)){
            // no need continuing iterating because next date will also be after endDate
            break
          } else if(date.before(startDate)){
            has_before_start_date_trans = true
          }
          else {
            // transaction date is in studied period
            has_during_period_trans = true
            if(has_before_start_date_trans){
              return ActivitySegment.ACTIVE.toString
            }
            return ActivitySegment.NEW.toString
          }
        }
      }
      if (has_before_start_date_trans){
        return ActivitySegment.INACTIVE.toString
      }
    }
    return ActivitySegment.UNDEFINED.toString
  }


  def computeSegmentation(customers: Dataset[String], transactions: Dataset[Transaction],
                          startDate: Date, endDate: Date): DataFrame = {

    val studied_transactions = transactions.filter(col("date") <= lit(endDate) )

    val transactions_per_cust_df = studied_transactions
      .groupBy("customerId")
      .agg(collect_list("date"))
      .select(
        col("customerId").alias("t_customerId"),
        col("collect_list(date)").alias("t_dates")
      )


    var joined_transaction_per_cust_df = customers
      .join(
        transactions_per_cust_df,
        customers.col("customerId") === transactions_per_cust_df.col("t_customerId"),
        "left_outer"
    )

    val getActivitySegmentUdf = udf[String, Seq[Date], Date, Date](getActivitySegment)

    joined_transaction_per_cust_df = joined_transaction_per_cust_df
      .withColumn(
      "activitySegment",
        getActivitySegmentUdf(col("t_dates"), lit(startDate), lit(endDate))
    )

    joined_transaction_per_cust_df = joined_transaction_per_cust_df.select(
      col("customerId"),
      col("activitySegment"))

    return joined_transaction_per_cust_df
  }



  private def saveSegmentation(segmentation: DataFrame, outputFile: String): Unit = {
    segmentation.select(col("customerId"), col("activitySegment"))
      .write
      .format("csv")
      .mode("overwrite")
      .option("sep", ";")
      .save(outputFile)
  }

  private def createSparkSession(): SparkSession = {
    SparkSession.builder().config("spark.master", "local[2]").getOrCreate()
  }



}
