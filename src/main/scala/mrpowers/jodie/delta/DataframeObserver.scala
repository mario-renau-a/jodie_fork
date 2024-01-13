package mrpowers.jodie.delta

import mrpowers.jodie.delta.ProtocolPrinter.protocolPrint
import org.apache.spark.sql.functions.{array, column, lit, mean}
import org.apache.spark.sql._

import scala.Console.{BLUE, GREEN, RESET}

// Observer Interface
trait Observer extends Serializable {
  def update(df: DataFrame)
}

trait SingleColumnActionObserver extends Observer {
  def updateAndReturn(df: DataFrame): DataFrame

  def chainObservers(df: DataFrame, observers: List[SingleColumnActionObserver]): Unit = {
    observers.foldLeft(df) {
      (dataFrame, observer) => observer.updateAndReturn(dataFrame)
    }
  }


}

class UppercaseStringColumnsObserver extends SingleColumnActionObserver {
  import org.apache.spark.sql.functions.upper
  import org.apache.spark.sql.types.StringType

  override def update(df: DataFrame): Unit = {
    val result = updateAndReturn(df)
    println("Modified DataFrame:\n")
    result.show(5)
    println("Made sure all string columns are uppercase.")
  }

  override def updateAndReturn(df: DataFrame): DataFrame = {
    df.schema.fields.foldLeft(df) { (dfUpdated, structField) =>
      structField.dataType match {
        case StringType => dfUpdated.withColumn(structField.name, upper(dfUpdated(structField.name)))
        case _ => dfUpdated
      }
    }
  }
}

// Base Observer abstract class
trait SingleActionObserver extends Observer {
  override def update(df: DataFrame): Unit = {
    val result = action(df)
    println(message(result))
  }
  def action(df: DataFrame): Any  // Perform whatever operation on the DataFrame
  def message(result: Any): String  // Convert the result to a message string
}

// Concrete Observer classes extending from SingleActionObserver
class RowCountObserver extends SingleActionObserver {
  override def action(df: DataFrame): Any = df.count()
  override def message(result: Any): String = s"Loaded DataFrame with $result rows."
}

class ProcessingTimeObserver extends SingleActionObserver {
  private var startTime: Long = System.currentTimeMillis()
  override def action(df: DataFrame): Any = {
    val endTime = System.currentTimeMillis()
    startTime = System.currentTimeMillis()
    endTime - startTime
  }
  override def message(result: Any): String = s"Data loading took $result ms."
}


class NullValueCountObserver extends SingleActionObserver {
  override def action(df: DataFrame): Any = {

    df.columns.map(colName => df.filter(df(colName).isNull).count()).sum
  }

  override def message(result: Any): String =
    s"There are $result null values in the DataFrame."
}

class ColumnValueFreqObserver(colNames: Seq[String]) extends SingleActionObserver {
  override def action(df: DataFrame): Any =
    colNames.flatMap(colName => df.groupBy(colName).count().collect().map(row => (colName, row.getLong(1))).toSeq)
  override def message(result: Any): String =
    result.asInstanceOf[Seq[(String, Long)]].map { case (colName, count) =>
      s"There are $count distinct values in column $colName."
    }.mkString("\n")
}

class AverageObserver(colNames: Seq[String]) extends SingleActionObserver {
  override def action(df: DataFrame): Any =
    colNames.map(colName => (colName, df.select(mean(colName)).first().getDouble(0)))
  override def message(result: Any): String =
    result.asInstanceOf[Seq[(String, Double)]].map { case (colName, avg) =>
      f"The average of column $colName is $avg%.2f."
    }.mkString("\n")
}

class AgeRangeObserver(minAge: Int, maxAge: Int) extends SingleColumnActionObserver {

  override def action(column: Column): Column = {
    column.between(minAge, maxAge)
  }

  override def message(result: Any): String = {
    val count = result.asInstanceOf[DataFrame].count
    s"There are $count records where age is between $minAge and $maxAge."
  }

  override def update(df: DataFrame): Unit = {
    val result = action(df)
    println(s"Ages in Range DataFrame:\n")
    result.show(5)
    println(message(result))
  }
}


// Subject Interface
trait Subject {
  private var observers: List[Observer] = List()
  def register(observer: Observer): Unit = observers = observer :: observers
  def unregister(observer: Observer): Unit = observers = observers.filter(_ != observer)
  def notifyObservers(df: DataFrame): Unit = observers.foreach(_.update(df))
}

// Base DataLoader abstract class
abstract class BaseDataLoader(val format: String) extends Subject {
  def load(spark: SparkSession, path: String, options: Map[String, String] = Map()): DataFrame = {
    val df = spark.read.options(options).format(format).load(path)
    notifyObservers(df)
    df
  }
}

// DataLoader implementations
class CSVDataLoader extends BaseDataLoader("csv") {
  override def load(spark: SparkSession, path: String, options: Map[String, String] = Map()): DataFrame = {
    val defaultOptions = Map("header" -> "true", "multiline" -> "false", "inferSchema" -> "true")
    super.load(spark, path, defaultOptions ++ options)
  }
}

class JSONDataLoader extends BaseDataLoader("json") {
  override def load(spark: SparkSession, path: String, options: Map[String, String] = Map()): DataFrame = {
    val defaultOptions = Map("multiline" -> "false")
    super.load(spark, path, defaultOptions ++ options)
  }
}

object DataLoaderFactory {
  def getDataLoader(source: String): BaseDataLoader = source match {
    case "csv" => new CSVDataLoader
    case "json" => new JSONDataLoader
    case _ => throw new IllegalArgumentException("Unsupported source.")
  }
}

object ProtocolPrinter {
  def protocolPrint(protocol: String): Unit = println(s"==== $protocol ====")
}

object DataframeObserverApp extends App {
  val spark = SparkSession.builder().master("local").appName("Factory and Observer Patterns").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  val observers: List[Observer] = List(
    new RowCountObserver,
    new ProcessingTimeObserver,
    //new ColumnStatsObserver(Seq("age", "id")),
    new NullValueCountObserver,
    new ColumnValueFreqObserver(Seq("age", "name")),
    new AverageObserver(Seq("age", "id")),
    new UppercaseStringColumnsObserver,
    new AgeRangeObserver(18, 90),
  )

  def applyObservers(dataLoader: BaseDataLoader, operation: (BaseDataLoader, Observer) => Unit): Unit = {
    println(s"${GREEN}Applying observers...${RESET}")
    observers.foreach(observer => operation(dataLoader, observer))
  }

  def loadData(dataLoader: BaseDataLoader, path: String, options: Map[String, String] = Map()): DataFrame = {
    println(s"${BLUE}Loading Data from $path...${RESET}")
    dataLoader.load(spark, path, options)
  }

  protocolPrint("CSV Data Operations")
  val csvDataLoader = DataLoaderFactory.getDataLoader("csv")
  applyObservers(csvDataLoader, (dataLoader: BaseDataLoader, observer: Observer) => dataLoader.register(observer))
  loadData(csvDataLoader, "data/csv", Map("header" -> "true")).cache()
  applyObservers(csvDataLoader, (dataLoader: BaseDataLoader, observer: Observer) => dataLoader.unregister(observer))

  protocolPrint("JSON Data Operations")
  val jsonDataLoader = DataLoaderFactory.getDataLoader("json")
  applyObservers(jsonDataLoader, (dataLoader: BaseDataLoader, observer: Observer) => dataLoader.register(observer))
  loadData(jsonDataLoader, "data/json").cache()
  applyObservers(jsonDataLoader, (dataLoader: BaseDataLoader, observer: Observer) => dataLoader.unregister(observer))

  println(s"${GREEN}Operation Completed${RESET}")
}

object GenCSVApp extends App {
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions.rand

  val spark = SparkSession.builder().appName("data generator").master("local").getOrCreate()

  // Define the number of rows, adjust as needed
  val numRows = 10000000

  val ageRange = (18 to 100).toList // Define age range

  // Generate numerical data and random name string
  val df = spark.range(0, numRows)
    .select((rand() * numRows).cast("int").alias("id"))
    .withColumn("name", functions.concat(lit("Name_"), column("id")))
    .withColumn("age", array(ageRange.map(lit(_)): _*)((rand() * (ageRange.size - 1)).cast("int")))

  df.write.mode(SaveMode.Append).option("header", "true").csv("data/csv")

  df.write.mode(SaveMode.Append).option("header", "true").json("data/json")
  spark.stop()
}