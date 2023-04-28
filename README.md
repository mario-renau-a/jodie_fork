# jodie

This library provides helpful Delta Lake and filesystem utility functions.

![jodie](images/jodie.jpeg)

## Accessing the library

Fetch the JAR file from Maven.

```scala
libraryDependencies += "com.github.mrpowers" %% "jodie" % "0.0.3"
```

You can find the spark-daria releases for different Scala versions:

* [Scala 2.12 versions here](https://repo1.maven.org/maven2/com/github/mrpowers/jodie_2.12/)
* [Scala 2.13 versions here](https://repo1.maven.org/maven2/com/github/mrpowers/jodie_2.13/)

## Delta Helpers

### Type 2 SCDs

This library provides an opinionated, conventions over configuration, approach to Type 2 SCD management.  Let's look at an example before covering the conventions required to take advantage of the functionality.

Suppose you have the following SCD table with the `pkey` primary key:

```
+----+-----+-----+----------+-------------------+--------+
|pkey|attr1|attr2|is_current|     effective_time|end_time|
+----+-----+-----+----------+-------------------+--------+
|   1|    A|    A|      true|2019-01-01 00:00:00|    null|
|   2|    B|    B|      true|2019-01-01 00:00:00|    null|
|   4|    D|    D|      true|2019-01-01 00:00:00|    null|
+----+-----+-----+----------+-------------------+--------+
```

You'd like to perform an upsert with this data:

```
+----+-----+-----+-------------------+
|pkey|attr1|attr2|     effective_time|
+----+-----+-----+-------------------+
|   2|    Z| null|2020-01-01 00:00:00| // upsert data
|   3|    C|    C|2020-09-15 00:00:00| // new pkey
+----+-----+-----+-------------------+
```

Here's how to perform the upsert:

```scala
Type2Scd.upsert(deltaTable, updatesDF, "pkey", Seq("attr1", "attr2"))
```

Here's the table after the upsert:

```
+----+-----+-----+----------+-------------------+-------------------+
|pkey|attr1|attr2|is_current|     effective_time|           end_time|
+----+-----+-----+----------+-------------------+-------------------+
|   2|    B|    B|     false|2019-01-01 00:00:00|2020-01-01 00:00:00|
|   4|    D|    D|      true|2019-01-01 00:00:00|               null|
|   1|    A|    A|      true|2019-01-01 00:00:00|               null|
|   3|    C|    C|      true|2020-09-15 00:00:00|               null|
|   2|    Z| null|      true|2020-01-01 00:00:00|               null|
+----+-----+-----+----------+-------------------+-------------------+
```

You can leverage the upsert code if your SCD table meets these requirements:

* Contains a unique primary key column
* Any change in an attribute column triggers an upsert
* SCD logic is exposed via `effective_time`, `end_time` and `is_current` column

`merge` logic can get really messy, so it's easiest to follow these conventions.  See [this blog post](https://mungingdata.com/delta-lake/type-2-scd-upserts/) if you'd like to build a SCD with custom logic.

### Kill Duplicates

The function `killDuplicateRecords` deletes all the duplicated records from a table given a set of columns.

Suppose you have the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson| # duplicate
|   2|    Maria|   Willis|
|   3|     Jose| Travolta| # duplicate
|   4|   Benito|  Jackson| # duplicate
|   5|     Jose| Travolta| # duplicate
|   6|    Maria|     Pitt|
|   9|   Benito|  Jackson| # duplicate
+----+---------+---------+
```

We can Run the following function to remove all duplicates:

```scala
DeltaHelpers.killDuplicateRecords(
  deltaTable = deltaTable, 
  duplicateColumns = Seq("firstname","lastname")
)
```

The result of running the previous function is the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   2|    Maria|   Willis|
|   2|    Maria|     Pitt| 
+----+---------+---------+
```

### Remove Duplicates

The functions `removeDuplicateRecords` deletes duplicates but keeps one occurrence of each record that was duplicated.
There are two versions of that function, lets look an example of each,

#### Let’s see an example of how to use the first version:

Suppose you have the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   2|    Maria|   Willis|
|   3|     Jose| Travolta|
|   4|   Benito|  Jackson|
|   1|   Benito|  Jackson| # duplicate
|   5|     Jose| Travolta| # duplicate
|   6|    Maria|   Willis| # duplicate
|   9|   Benito|  Jackson| # duplicate
+----+---------+---------+
```
We can Run the following function to remove all duplicates:

```scala
DeltaHelpers.removeDuplicateRecords(
  deltaTable = deltaTable,
  primaryKey = "id",
  duplicateColumns = Seq("firstname","lastname")
)
```

The result of running the previous function is the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   2|    Maria|   Willis|
|   3|     Jose| Travolta|
|   4|   Benito|  Jackson| 
+----+---------+---------+
```

#### Now let’s see an example of how to use the second version:

Suppose you have a similar table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   2|    Maria|   Willis|
|   3|     Jose| Travolta| # duplicate
|   4|   Benito|  Jackson| # duplicate
|   1|   Benito|  Jackson| # duplicate
|   5|     Jose| Travolta| # duplicate
|   6|    Maria|     Pitt|
|   9|   Benito|  Jackson| # duplicate
+----+---------+---------+
```

This time the function takes an additional input parameter, a primary key that will be used to sort 
the duplicated records in ascending order and remove them according to that order.

```scala
DeltaHelpers.removeDuplicateRecords(
  deltaTable = deltaTable,
  primaryKey = "id",
  duplicateColumns = Seq("firstname","lastname")
)
```

The result of running the previous function is the following:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson|
|   2|    Maria|   Willis|
|   3|     Jose| Travolta|
|   6|    Maria|     Pitt|
+----+---------+---------+
```

These functions come in handy when you are doing data cleansing.

### Copy Delta Table

This function takes an existing delta table and makes a copy of all its data, properties,
and partitions to a new delta table. The new table could be created based on a specified path or
just a given table name. 

Copying does not include the delta log, which means that you will not be able to restore the new table to an old version of the original table.

Here's how to perform the copy to a specific path:

```scala
DeltaHelpers.copyTable(deltaTable = deltaTable, targetPath = Some(targetPath))
```

Here's how to perform the copy using a table name:

```scala
DeltaHelpers.copyTable(deltaTable = deltaTable, targetTableName = Some(tableName))
```

Note the location where the table will be stored in this last function call 
will be based on the spark conf property `spark.sql.warehouse.dir`.

### Latest Version of Delta Table
The function `latestVersion` return the latest version number of a table given its storage path. 

Here's how to use the function:
```scala
DeltaHelpers.latestVersion(path = "file:/path/to/your/delta-lake/table")
```

### Insert Data Without Duplicates
The function `appendWithoutDuplicates` inserts data into an existing delta table and prevents data duplication in the process.
Let's see an example of how it works.

Suppose we have the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson|
|   4|    Maria|     Pitt|
|   6|  Rosalia|     Pitt|
+----+---------+---------+
```
And we want to insert this new dataframe:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   6|  Rosalia|     Pitt| # duplicate
|   2|    Maria|   Willis|
|   3|     Jose| Travolta|
|   4|    Maria|     Pitt| # duplicate
+----+---------+---------+
```

We can use the following function to insert new data and avoid data duplication:
```scala
DeltaHelpers.appendWithoutDuplicates(
  deltaTable = deltaTable,
  appendData = newDataDF, 
  compositeKey = Seq("firstname","lastname")
)
```

The result table will be the following:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson|
|   4|    Maria|     Pitt|
|   6|  Rosalia|     Pitt|
|   2|    Maria|   Willis|
|   3|     Jose| Travolta| 
+----+---------+---------+
```
### Generate MD5 from columns

The function `withMD5Columns` appends a md5 hash of specified columns to the DataFrame. This can be used as a unique key 
if the selected columns form a composite key. Here is an example

Suppose we have the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson|
|   4|    Maria|     Pitt|
|   6|  Rosalia|     Pitt|
+----+---------+---------+
```

We use the function in this way:
```scala
DeltaHelpers.withMD5Columns(
  dataFrame = inputDF,
  cols = List("firstname","lastname"),
  newColName = "unique_id")
)
```

The result table will be the following:
```
+----+---------+---------+----------------------------------+
|  id|firstname| lastname| unique_id                        |
+----+---------+---------+----------------------------------+
|   1|   Benito|  Jackson| 3456d6842080e8188b35f515254fece8 |
|   4|    Maria|     Pitt| 4fd906b56cc15ca517c554b215597ea1 |
|   6|  Rosalia|     Pitt| 3b3814001b13695931b6df8670172f91 |
+----+---------+---------+----------------------------------+
```

You can use this function with the columns identified in findCompositeKeyCandidate to append a unique key to the DataFrame.

### Find Composite Key
This function `findCompositeKeyCandidate` helps you find a composite key that uniquely identifies the rows your Delta table. 
It returns a list of columns that can be used as a composite key. i.e:

Suppose we have the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson|
|   4|    Maria|     Pitt|
|   6|  Rosalia|     Pitt|
+----+---------+---------+
```

Now execute the function:
```scala
val result = DeltaHelpers.findCompositeKeyCandidate(
  deltaTable = deltaTable,
  excludeCols = Seq("id")
)
```

The result will be the following:

```scala
Seq("firstname","lastname")
```

## Delta File Sizes

The `deltaFileSizes` function returns a `Map[String,Long]` that contains the total size in bytes, the amount of files and the
average file size for a given Delta Table.

Suppose you have the following Delta Table, partitioned by `col1`:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   A|   A|
|   2|   A|   B|
+----+----+----+
```

Running `DeltaHelpers.deltaFileSizes(deltaTable)` on that table will return:

```scala
Map("size_in_bytes" -> 1320,
  "number_of_files" -> 2,
  "average_file_size_in_bytes" -> 660)
```

## Change Data Feed Helpers

### CASE I - When Delta aka Transaction Log gets purged

`getVersionsForAvailableDeltaLog` - helps you find the versions within the `[startingVersion,endingVersion]`range for which Delta Log is present and CDF read is enabled (only for the start version) and possible
```scala
ChangeDataFeedHelper(deltaPath, 0, 5).getVersionsForAvailableDeltaLog
```
The result will return the same versions `Some(0,5)` if Delta Logs are present. Otherwise, it will return say `Some(10,15)` - the earliest queryable start version and latest snapshot version as ending version. If at any point within versions it finds that EDR is disabled, it returns a `None`.

`readCDFIgnoreMissingDeltaLog` - Returns an Option of Spark Dataframe for all versions provided by the above method
```scala
ChangeDataFeedHelper(deltaPath, 11, 13).readCDFIgnoreMissingDeltaLog.get.show(false)

+---+------+---+----------------+---------------+-------------------+
|id |gender|age|_change_type    |_commit_version|_commit_timestamp  |
+---+------+---+----------------+---------------+-------------------+
|4  |Female|25 |update_preimage |11             |2023-03-13 14:21:58|
|4  |Other |45 |update_postimage|11             |2023-03-13 14:21:58|
|2  |Male  |45 |update_preimage |13             |2023-03-13 14:22:05|
|2  |Other |67 |update_postimage|13             |2023-03-13 14:22:05|
|2  |Other |67 |update_preimage |12             |2023-03-13 14:22:01|
|2  |Male  |45 |update_postimage|12             |2023-03-13 14:22:01|
+---+------+---+----------------+---------------+-------------------+
```
Resultant Dataframe is the same as the result of CDF Time Travel query
### CASE II - When CDC data gets purged in `_change_data` directory

`getVersionsForAvailableCDC` - helps you find the versions within the `[startingVersion,endingVersion]`range for which underlying CDC data is present under `_change_data` directory. Call this method when java.io.FileNotFoundException is encountered during time travel
```scala
ChangeDataFeedHelper(deltaPath, 0, 5).getVersionsForAvailableCDC
```
The result will return the same versions `Some(0,5)` if CDC data is present for the given versions under `_change_data` directory. Otherwise, it will return `Some(2,5)` - the earliest queryable start version for which CDC is present and given ending version. If no version is found that has CDC data available, it returns a `None`.
 

`readCDFIgnoreMissingCDC` - Returns an Option of Spark Dataframe for all versions provided by the above method
```scala
ChangeDataFeedHelper(deltaPath, 11, 13).readCDFIgnoreMissingCDC.show(false)

+---+------+---+----------------+---------------+-------------------+
|id |gender|age|_change_type    |_commit_version|_commit_timestamp  |
+---+------+---+----------------+---------------+-------------------+
|4  |Female|25 |update_preimage |11             |2023-03-13 14:21:58|
|4  |Other |45 |update_postimage|11             |2023-03-13 14:21:58|
|2  |Male  |45 |update_preimage |13             |2023-03-13 14:22:05|
|2  |Other |67 |update_postimage|13             |2023-03-13 14:22:05|
|2  |Other |67 |update_preimage |12             |2023-03-13 14:22:01|
|2  |Male  |45 |update_postimage|12             |2023-03-13 14:22:01|
+---+------+---+----------------+---------------+-------------------+
```
Resultant Dataframe is the same as the result of CDF Time Travel query

### CASE III - Enable-Disable-Re-enable CDF

`getRangesForCDFEnabledVersions`- Skip all versions for which CDF was disabled and get all ranges for which CDF was enabled and time travel is possible within a `[startingVersion,endingVersion]`range
```scala
 ChangeDataFeedHelper(writePath, 0, 30).getRangesForCDFEnabledVersions
```
The result will look like `List((0, 3), (7, 8), (12, 20))` signifying all version ranges for which CDF is enabled. The function `getRangesForCDFDisabledVersions` returns exactly same `List` but this time it returns disabled version ranges.

`readCDFIgnoreMissingRangesForEDR`- Returns an Option of unionised Spark Dataframe for all version ranges provided by the above method
```scala
 ChangeDataFeedHelper(writePath, 0, 30).readCDFIgnoreMissingRangesForEDR
+---+------+---+----------------+---------------+-------------------+
|id |gender|age|_change_type    |_commit_version|_commit_timestamp  |
+---+------+---+----------------+---------------+-------------------+
|2  |Male  |25 |update_preimage |2              |2023-03-13 14:40:48|
|2  |Male  |100|update_postimage|2              |2023-03-13 14:40:48|
|1  |Male  |25 |update_preimage |1              |2023-03-13 14:40:44|
|1  |Male  |35 |update_postimage|1              |2023-03-13 14:40:44|
|2  |Male  |100|update_preimage |3              |2023-03-13 14:40:52|
|2  |Male  |101|update_postimage|3              |2023-03-13 14:40:52|
|1  |Male  |25 |insert          |0              |2023-03-13 14:40:34|
|2  |Male  |25 |insert          |0              |2023-03-13 14:40:34|
|3  |Female|35 |insert          |0              |2023-03-13 14:40:34|
|2  |Male  |101|update_preimage |8              |2023-03-13 14:41:07|
|2  |Other |66 |update_postimage|8              |2023-03-13 14:41:07|
|2  |Other |66 |update_preimage |13             |2023-03-13 14:41:24|
|2  |Other |67 |update_postimage|13             |2023-03-13 14:41:24|
|2  |Other |67 |update_preimage |14             |2023-03-13 14:41:27|
|2  |Other |345|update_postimage|14             |2023-03-13 14:41:27|
|2  |Male  |100|update_preimage |20             |2023-03-13 14:41:46|
|2  |Male  |101|update_postimage|20             |2023-03-13 14:41:46|
|4  |Other |45 |update_preimage |15             |2023-03-13 14:41:30|
|4  |Female|678|update_postimage|15             |2023-03-13 14:41:30|
|1  |Other |55 |update_preimage |18             |2023-03-13 14:41:40|
|1  |Male  |35 |update_postimage|18             |2023-03-13 14:41:40|
|2  |Other |345|update_preimage |19             |2023-03-13 14:41:43|
|2  |Male  |100|update_postimage|19             |2023-03-13 14:41:43|
+---+------+---+----------------+---------------+-------------------+
```
Resultant Dataframe is the same as the result of CDF Time Travel query but this time it will only have CDC for enabled versions ignoring all versions for which CDC was disabled.
### Dry Run
`dryRun`- This method works as a fail-safe to see if there are any CDF-related issues. If it doesn't throw any errors, then you can be certain the above-mentioned issues do not occur in your Delta Table for the given versions. When it does, it throws either an AssertionError or an IllegalStateException with appropriate error message

`readCDF`- Plain old time travel query, this is literally the method definition, that's it
```scala
spark.read.format("delta").option("readChangeFeed","true").option("startingVersion",0).("endingVersion",20).load(gcs_path)
```
Pair `dryRun` with `readCDF` to detect any CDF errors in your Delta Table
```scala
ChangeDataFeedHelper(writePath, 9, 13).dryRun().readCDF
```
If no error found, it will return a similar Spark Dataframe with CDF between given versions. 

## Operation Metric Helpers

### Count Metrics on Delta Table between 2 versions
This function displays all count metric stored in the Delta Logs across versions for the entire Delta Table. It skips versions which do not record 
these count metrics and presents a unified view. It shows the growth of a Delta Table by providing the record counts - 
**deleted**, **updated** and **inserted** against a **version**. For a **merge** operation, we additionally have a source dataframe to tally
with as **source rows = (deleted + updated + inserted) rows**.  Please note that you need to have enough Driver Memory
for processing the Delta Logs at driver level.
```scala
OperationMetricHelper(path,0,6).getCountMetricsAsDF()
```
The result will be following:
```scala
+-------+-------+--------+-------+-----------+
|version|deleted|inserted|updated|source_rows|
+-------+-------+--------+-------+-----------+
|6      |0      |108     |0      |108        |
|5      |12     |0       |0      |0          |
|4      |0      |0       |300    |300        |
|3      |0      |100     |0      |100        |
|2      |0      |150     |190    |340        |
|1      |0      |0       |200    |200        |
|0      |0      |400     |0      |400        |
+-------+-------+--------+-------+-----------+
```
### Count Metrics at partition level of Delta Table
This function provides the same count metrics as the above function, but this time at a partition level. If operations 
like **MERGE, DELETE** and **UPDATE** are executed **at a partition level**, then this function can help in visualizing count 
metrics for such a partition. However, **it will not provide correct count metrics if these operations are performed 
across partitions**. This is because Delta Log does not store this information at a log level and hence, need to be 
implemented separately (we intend to take this up in future). Please note that you need to have enough Driver Memory
for processing the Delta Logs at driver level.
```scala
OperationMetricHelper(path).getCountMetricsAsDF(
  Some(" country = 'USA' and gender = 'Female'"))

// The same metric can be obtained generally without using spark dataframe
def getCountMetrics(partitionCondition: Option[String] = None)
                    : Seq[(Long, Long, Long, Long, Long)]
```
The result will be following:
```scala
+-------+-------+--------+--------+-----------+
|version|deleted|inserted| updated|source_rows|
+-------+-------+--------+--------+-----------+
|     27|      0|       0|20635530|   20635524|
|     14|      0|       0| 1429460|    1429460|
|     13|      0|       0| 4670450|    4670450|
|     12|      0|       0|20635530|   20635524|
|     11|      0|       0| 5181821|    5181821|
|     10|      0|       0| 1562046|    1562046|
|      9|      0|       0| 1562046|    1562046|
|      6|      0|       0|20635518|   20635512|
|      3|      0|       0| 5181821|    5181821|
|      0|      0|56287990|       0|   56287990|
+-------+-------+--------+--------+-----------+
```
Supported Partition condition types
```scala
// Single Partition
Some(" country = 'USA'")
// Multiple Partition with AND condition. OR is not supported.
Some(" country = 'USA' and gender = 'Female'")
// Without Single Quotes
Some(" country = USA and gender = Female")
```

## How to contribute
We welcome contributions to this project, to contribute checkout our [CONTRIBUTING.md](CONTRIBUTING.md) file.

## How to build the project

### pre-requisites
* SBT 1.8.2
* Java 8
* Scala 2.12.12

### Building

To compile, run
`sbt compile`

To test, run
`sbt test`

To generate artifacts, run
`sbt package`

## Project maintainers

* Matthew Powers aka [MrPowers](https://github.com/MrPowers)
* Brayan Jules aka [brayanjuls](https://github.com/brayanjuls)

## More about Jodie

See [this video](https://www.youtube.com/watch?v=llHKvaV0scQ) for more info about the awesomeness of Jodie!
