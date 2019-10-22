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

    val dataPath = "data" // = args(0)
    val outputFile = "output/"//args(1)
    val endDate = "2019-10-23"//args(2)
    val segmentationPeriod = 4 //args(3)

    val session = createSparkSession()

    run(session, dataPath, outputFile, LocalDate.parse(endDate), segmentationPeriod.toInt)

    val idUdf = udf { Random.alphanumeric.take(7).map(_.toLower).mkString }

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
    //???
  }


  private def loadTransactions(session: SparkSession, dataPath: String): Dataset[Transaction] = {
    import session.implicits._

    val transactionDF = session.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(dataPath + "/transactions/transactions.csv")
    val transactions = transactionDF.as[Transaction]
    return transactions
  }

  private def loadCustomers(session: SparkSession, dataPath: String): Dataset[String] = {
    import session.implicits._

    val customerDF = session.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(dataPath + "/customers/customerIds.csv")
    val customers = customerDF.as[String]
    return customers
  }



  private def getActivitySegment(transaction_dates: Seq[Date], startDate:Date, endDate: Date): String = {
//  private def getActivitySegment(transaction_dates: Seq[Date]): String = {
    //println(transaction_dates)
    if(transaction_dates != null){

      val sorted_dates = transaction_dates.sortBy(_.getTime)

      //val after_end_date= get_after_dates()
      var has_undef_trans : Boolean = false
      var has_inac_trans: Boolean = false
      var has_active_trans: Boolean = false
      breakable{
        for (date <- sorted_dates){
          println(date.toString)
          if(date.after(endDate)){
            println("after")

            has_inac_trans = true
            break
            //break the loop
          } else if(date.before(startDate)){
            println("before")
            has_inac_trans = true
          }
          else {
            has_active_trans = true
            if(has_inac_trans){
              return "HAS_ACTIVE TRANS"
            }
            return "NEW"

          }
        }
      }
      println("outside loop")
      if (has_inac_trans){
        println("inactive")
        return "INACTIVE"
      }

    }
    return "UNDEF"

    //return ActivitySegment.UNDEFINED
//    return "ActivitySegment.UNDEFINED"
  }




  def computeSegmentation(customers: Dataset[String], transactions: Dataset[Transaction],
                          startDate: Date, endDate: Date): DataFrame = {
//    val studied_transactions = transactions
    val studied_transactions = transactions.filter(col("date") <= lit(endDate) )

    val transactions_per_cust_df = studied_transactions.groupBy("customerId")
      .agg(collect_list("date"))
      .select(col("customerId").alias("t_customerId"), col("collect_list(date)").alias("t_dates"))


    var joined_transaction_per_cust_df = customers.join(transactions_per_cust_df,
        customers.col("customerId") === transactions_per_cust_df.col("t_customerId"), "left_outer"
    )



//    val getActivitySegmentUdf = udf[String, Seq[Date]](getActivitySegment)
    val getActivitySegmentUdf = udf[String, Seq[Date], Date, Date](getActivitySegment)


    joined_transaction_per_cust_df = joined_transaction_per_cust_df.filter("t_dates is not null")
    joined_transaction_per_cust_df = joined_transaction_per_cust_df.withColumn(
//      "activitySegment", getActivitySegmentUdf(col("t_dates"))
      "activitySegment", getActivitySegmentUdf(col("t_dates"), lit(startDate), lit(endDate))
    )

    println(" after join")
    joined_transaction_per_cust_df.show(false)
    joined_transaction_per_cust_df.printSchema()
    println(joined_transaction_per_cust_df.count())
    println("cust")
    println(customers.count())



    return joined_transaction_per_cust_df
  }







  private def saveSegmentation(segmentation: DataFrame, outputFile: String): Unit = {
    segmentation.select(col("customerId"), col("activitySegment")).show(false)
    segmentation.select(col("customerId"), col("activitySegment")).write
      .format("csv")
      .mode("overwrite")
      .option("sep", ";").save("output")
    return
  }

  private def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder().config("spark.master", "local[2]").getOrCreate()


    return spark
  }



}
