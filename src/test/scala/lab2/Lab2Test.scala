
package lab2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class Lab2Test extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("Lab2Test")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  test("Task 2: read CSV and correctly identify columns and date") {
    val file = Files.createTempFile("stocks", ".csv")

    try {
      val csv =
        "date,ISE_TL,ISE_USD,DAX\n" +
          "2011-02-22,0.01,0.02,0.03\n" +
          "2011-02-21,0.04,0.05,0.06\n"

      Files.write(file, csv.getBytes(StandardCharsets.UTF_8))

      val df = Task2ReadData.read(spark, file.toString)

      assert(df.count() == 2)
      assert(df.columns.toSeq == Seq("date", "ISE_TL", "ISE_USD", "DAX"))
      assert(df.schema("date").dataType == DateType)
      assert(df.filter("date IS NULL").count() == 0)
    } finally {
      Files.deleteIfExists(file)
    }
  }

  test("Task 3: print column names in order") {
    val session = spark
    import session.implicits._

    val df = Seq((1, 2, 3))
      .toDF("date", "ISE_TL", "ISE_USD")

    assert(Task3PrintHeader.print(df) == "date, ISE_TL, ISE_USD")
  }

  test("Task 4: display first five rows") {
    val session = spark
    import session.implicits._

    val df = (1 to 10)
      .map(i => (i, s"value_$i"))
      .toDF("id", "value")

    val output = new ByteArrayOutputStream()

    Console.withOut(new PrintStream(output)) {
      Task4ShowData.show(df)
    }

    val text = output.toString("UTF-8")

    assert(text.contains("value_1"))
    assert(text.contains("value_5"))
    assert(!text.contains("value_6"))
  }

  test("Task 5: sort dates from newest to oldest") {
    val session = spark
    import session.implicits._

    val df = Seq(
      ("2011-02-20", 1.0),
      ("2011-02-22", 2.0),
      ("2011-02-21", 3.0)
    ).toDF("date", "value")

    val sorted = Task5SortByDate.sort(df)

    val dates = sorted.select("date")
      .as[String]
      .collect()
      .toSeq

    assert(dates == Seq(
      "2011-02-22",
      "2011-02-21",
      "2011-02-20"
    ))
  }

  test("Task 6: remove EM column") {
    val session = spark
    import session.implicits._

    val df = Seq(
      (1, 0.01, 0.02),
      (2, 0.03, 0.04)
    ).toDF("id", "DAX", "EM")

    val result = Task6RemoveEM.remove(df)

    assert(result.columns.toSeq == Seq("id", "DAX"))
    assert(result.count() == 2)
    assert(df.columns.contains("EM"))
  }

  test("Task 7: sort NIKKEI values from highest to lowest") {
    val session = spark
    import session.implicits._

    val df = Seq(
      ("2011-02-20", 0.02),
      ("2011-02-21", 0.08),
      ("2011-02-22", -0.01)
    ).toDF("date", "NIKKEI")

    val result = Task7SortByNikkei.sort(df)

    val values = result
      .select("NIKKEI")
      .as[Double]
      .collect()
      .toSeq

    assert(values == Seq(0.08, 0.02, -0.01))
  }

  test("Task 8: calculate average of DAX and FTSE") {
    val session = spark
    import session.implicits._

    val df = Seq(
      (0.02, 0.04),
      (0.10, 0.20),
      (-0.04, 0.02)
    ).toDF("DAX", "FTSE")

    val result = Task8AddAverage.add(df)

    val averages = result
      .select("DAX_FTSE_AVG")
      .as[Double]
      .collect()
      .toSeq

    val expected = Seq(0.03, 0.15, -0.01)

    assert(averages.zip(expected).forall {
      case (actual, expectedValue) =>
        math.abs(actual - expectedValue) < 1e-9
    })

    assert(result.columns.contains("DAX"))
    assert(result.columns.contains("FTSE"))
  }


  test("Task 9: find highest DAX and NIKKEI values") {
    val session = spark
    import session.implicits._

    val df = Seq(
      (0.02, -0.01),
      (0.08, 0.05),
      (-0.03, 0.12),
      (0.04, 0.03)
    ).toDF("DAX", "NIKKEI")

    val highestDAX = Task9HighestValue.highest(df, "DAX")
    val highestNIKKEI = Task9HighestValue.highest(df, "NIKKEI")

    assert(math.abs(highestDAX - 0.08) < 1e-9)
    assert(math.abs(highestNIKKEI - 0.12) < 1e-9)
  }

  test("Task 10: save DataFrame to CSV") {
    val session = spark
    import session.implicits._

    val df = Seq(
      (1, 0.02, 0.04),
      (2, 0.06, 0.08)
    ).toDF("id", "DAX", "FTSE")

    val outputDir = Files.createTempDirectory("lab2-output")

    try {
      Task10SaveData.save(df, outputDir.toString)

      val saved = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(outputDir.toString)

      assert(saved.count() == 2)
      assert(saved.columns.toSeq == Seq("id", "DAX", "FTSE"))
    } finally {
      val stream = Files.walk(outputDir)

      try {
        import scala.jdk.CollectionConverters._

        stream.iterator().asScala.toSeq
          .sortBy(_.getNameCount)
          .reverse
          .foreach(Files.deleteIfExists(_))
      } finally {
        stream.close()
      }
    }
  }
}
