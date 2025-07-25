package com.sagarshingare

import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.pdmodel.encryption.InvalidPasswordException
import org.apache.pdfbox.text.PDFTextStripper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.io.{File, FilenameFilter}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer

object HDFCBatchProcessor {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.native.lib", "false")

    val spark = SparkSession.builder()
      .appName("HDFC Credit Card Batch Processor")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Input params
    val inputFolder = "data_set/banks_statements"
    val categoryCSV = "data_set/banks_statements/category_mapping.csv"
    val pdfPassword = "SAGA1801"

    // Load and broadcast category mapping
    val categoryMap = spark.read.option("header", "true")
      .csv(categoryCSV)
      .withColumn("keyword", lower(trim(col("keyword"))))
      .withColumn("category", trim(col("category")))
      .collect()
      .map(row => row.getString(0) -> row.getString(1))
      .toMap

    val broadcastMap = spark.sparkContext.broadcast(categoryMap)

    val categorizeUDF = udf((desc: String) => {
      val lowerDesc = desc.toLowerCase
      broadcastMap.value.collectFirst {
        case (k, v) if lowerDesc.contains(k) => v
      }.getOrElse("Others")
    })

    // Schema definition
    val schema = StructType(List(
      StructField("file", StringType),
      StructField("date", StringType),
      StructField("description", StringType),
      StructField("amount", DoubleType)
    ))

    // Process all PDFs in folder
    val files = new File(inputFolder)
      .listFiles(new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = name.toLowerCase.endsWith(".pdf")
      })

    val allRows = files.flatMap { file =>
      try {
        val text = extractTextFromPDF(file.getAbsolutePath, pdfPassword)
        val section = extractTransactionSection(text)
        val lines = section.split("\n")
          .map(_.trim)
          .filter(_.matches("""\d{2}/\d{2}/\d{4}.*\d+\.\d{2}"""))

        lines.map(parseLineToRow(file.getName))
      } catch {
        case _: InvalidPasswordException =>
          println(s"❌ Skipped ${file.getName}: Invalid password.")
          Array.empty[Row]
        case e: Exception =>
          println(s"❌ Error processing ${file.getName}: ${e.getMessage}")
          Array.empty[Row]
      }
    }

    val df = spark.createDataFrame(spark.sparkContext.parallelize(allRows), schema)

    val finalDF = df
      .withColumn("type", lit("Expense"))
      .withColumn("category", categorizeUDF(col("description")))

    // Show sample
    println(s"🔍 Count of Transactions: ${finalDF.count()}")
    finalDF.show(10, truncate = false)

    import org.apache.spark.sql.functions._

    val dfWithDateParts = finalDF
      .withColumn("txn_date", to_date(col("date"), "dd/MM/yyyy"))
      .withColumn("year", year(col("txn_date")))
      .withColumn("month", month(col("txn_date")))
      .withColumn("year_month", date_format(col("txn_date"), "yyyy-MM"))

    val availableMonths = dfWithDateParts
      .select("year_month")
      .distinct()
      .orderBy("year_month")

    println(s"🔍 Count of MonthsYears: ${availableMonths.count()}")

    availableMonths.show(30000,false)

    //missing month findout

    // Step 1: Extract year-month from date
    val dfWithDate = finalDF
      .withColumn("txn_date", to_date(col("date"), "dd/MM/yyyy"))
      .withColumn("year_month", date_format(col("txn_date"), "yyyy-MM"))

    // Step 2: Get min and max year_month from data
    val minMaxDates = dfWithDate.agg(
      min(col("txn_date")).as("min_date"),
      max(col("txn_date")).as("max_date")
    ).first()

    val start = minMaxDates.getDate(0).toLocalDate.withDayOfMonth(1)
    val end = minMaxDates.getDate(1).toLocalDate.withDayOfMonth(1)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM")

    // Step 3: Build full calendar list from min to max month
    val fullMonthList = ListBuffer[String]()
    var current = start
    while (!current.isAfter(end)) {
      fullMonthList += current.format(formatter)
      current = current.plusMonths(1)
    }
    val fullMonthDF = spark.createDataset(fullMonthList).toDF("year_month")

    // Step 4: Get distinct year_month from actual data
    val availableMonthsDF = dfWithDate.select("year_month").distinct()

    // Step 5: Find missing months
    val missingMonthsDF = fullMonthDF.except(availableMonthsDF).orderBy("year_month")

    // ✅ Show missing months
    println(s"🔍 Missing month-year statements: ${missingMonthsDF.count}")
    missingMonthsDF.show(30000,false)



    // Export full result
    /*val timestamp = java.time.LocalDateTime.now().toString.replaceAll("[:.]", "_")
    val outputPath = s"output/categorized_transactions_$timestamp"
    finalDF.coalesce(1)  // Forces single partition
      .write.option("header", "true")
      .mode("overwrite")
      .csv(outputPath)
     */

    spark.stop()
  }

  /** Extract full PDF text using password */
  def extractTextFromPDF(path: String, password: String): String = {
    val doc = PDDocument.load(new File(path), password)
    val stripper = new PDFTextStripper()
    val text = stripper.getText(doc)
    doc.close()
    text
  }

  /** Extract Domestic Transactions section only */
  def extractTransactionSection(text: String): String = {
    val start = "Domestic Transactions"
    val end = "Reward Points Summary"
    val sIdx = text.indexOf(start)
    val eIdx = text.indexOf(end)
    if (sIdx != -1 && eIdx != -1) text.substring(sIdx, eIdx) else ""
  }

  /** Parse transaction line into a Row */
  def parseLineToRow(fileName: String)(line: String): Row = {
    val pattern = """(\d{2}/\d{2}/\d{4})\s+(.+?)\s+([\d,]+\.\d{2})$""".r
    line match {
      case pattern(date, desc, amtStr) =>
        val amount = amtStr.replace(",", "").toDouble
        Row(fileName, date, desc.trim, amount)
      case _ =>
        Row(fileName, "NA", "Unparsed", 0.0)
    }
  }
}
