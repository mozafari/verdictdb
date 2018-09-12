# VerdictDB on Apache Spark


We will write a simple example program in Scala, the standard programming language for Apache Spark. To compile our example program, the following tools must be installed.

1. sbt: [sbt installation guide](https://www.scala-sbt.org/1.0/docs/Setup.html)


## Create an empty project

The following command creates a project that prints out "hello".

```bash
$ sbt new sbt/scala-seed.g8

A minimal Scala project.

name [Scala Seed Project]: hello-verdict

Template applied in ./hello-verdict
```

Move into the project directory: `cd hello-verdict`.

Remove the `src/test` directory, which we do not need: `rm -rf src/test`.



## Configure build setting to use Spark and VerdictDB

Add the following line in `build.sbt`, under the existing `import Dependencies._` line. As of the time of writing, the latest version of Apache Spark only supports Scala 2.11.

```scala
scalaVersion := "2.11.1"
```

Also, replace the existing dependency list with

```scala
libraryDependencies ++= Seq(
  scalaTest % Test,
  "org.verdictdb" % "verdictdb-core" % "0.5.4",
  "org.apache.spark" %% "spark-core" % "2.3.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"
)
```

This dependency declaration will let the compiler (`sbt` in our case) download relevant libraries automatically.


## Write an example program

Edit `src/main/scala/example/Hello.scala` as follows:

```scala
package example

import org.apache.spark.sql.SparkSession
import org.verdictdb.VerdictContext
import org.verdictdb.connection.SparkConnection
import scala.util.Random

object Hello extends App {
  val spark = SparkSession
    .builder()
    .appName("VerdictDB basic example")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val verdict = VerdictContext.fromSparkSession(spark)

  // prepare data
  prepareData(spark, verdict)

  // run a query and print its result
  val rs = verdict.sql("select count(*) from myschema.sales")
  rs.printCsv()

  // simply the following lines will be printed (the actual count value may vary)
  // c2
  // 950.0

  def prepareData(spark: SparkSession, verdict: VerdictContext): Unit = {
    // create a schema and a table
    spark.sql("DROP SCHEMA IF EXISTS myschema CASCADE")
    spark.sql("CREATE SCHEMA IF NOT EXISTS myschema")
    spark.sql("CREATE TABLE IF NOT EXISTS myschema.sales (product string, price double)")

    // insert 1000 rows
    val productList = List("milk", "egg", "juice")
    val rand = new Random()
    var query = "INSERT INTO myschema.sales VALUES"
    for (i <- 0 until 1000) {
      val randInt: Int = rand.nextInt(3)
      val product: String = productList(randInt)
      val price: Double = (randInt+2) * 10 + rand.nextInt(10)
      if (i == 0) {
        query = query + f" ('$product', $price%.0f)"
      } else {
        query = query + f", ('$product', $price%.0f)"
      }
    }
    spark.sql(query)

    verdict.sql("BYPASS DROP TABLE IF EXISTS myschema.sales_scramble")
    verdict.sql("BYPASS DROP SCHEMA IF EXISTS verdictdbtemp CASCADE")
    verdict.sql("BYPASS DROP SCHEMA IF EXISTS verdictdbmeta CASCADE")
    verdict.sql("CREATE SCRAMBLE myschema.sales_scramble FROM myschema.sales BLOCKSIZE 100")
  }
}
```


## Package and Submit

```bash
$ sbt package
$ spark-submit target/scala-2.11/Hello-assembly-0.1.0-SNAPSHOT.jar
```

This example program is available on [this public GitHub repository](https://github.com/verdictdb/verdictdb-tutorial). See the directory `verdictdb_on_spark`.