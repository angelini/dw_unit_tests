package com.angelini.dw_unit_tests

import java.nio.file.{Files, Path, Paths}

import com.angelini.dw_unit_tests.store.{FileStore, GCStore}

import scala.collection.JavaConverters._

class HasMetadata extends PartitionTestCase {
  override def run(partition: Partition): TestExecution.PartitionResult =
    partition.files.find(_.getFileName.endsWith("metadata")) match {
      case Some(_) => TestExecution.Success()
      case None => TestExecution.Failure("Metadata not found")
    }
}

object Main extends App {
  println(s"env: ${sys.env("GOOGLE_APPLICATION_CREDENTIALS")}")

  val schema = Schema(Seq(
    Column("id", ColumnType.IntT),
    Column("value", ColumnType.StringT)
  ))

  val tempDir = Files.createTempDirectory("dwut-")
  createFiles(tempDir, Seq(
    ("data/2018/01/01/metadata", Some("123")),
    ("data/2018/01/01/schema", Some(schema.toJSON)),
    ("data/2018/01/02/metadata", Some("456")),
    ("data/2018/01/02/schema", Some(schema.toJSON)),
    ("data/2018/01/03/other", None)
  ))

  val store = new FileStore
  val cases = Seq(new HasMetadata)

  val exampleFinder = new Finder(tempDir)
    .withFilter("data/*/*/*")
    .withSchemaParser((store, filePaths) => {
      filePaths.find(_.getFileName.endsWith("schema")).map(path => {
        Schema.fromJSON(store.read(path))
      })
    })

  for (partition <- exampleFinder.execute(store).partitions) {
    println(s"exampleFinder $partition")
  }

  val gcStore = new GCStore("test-list-version-files")
  val exampleGcsFinder = new Finder(Paths.get(""))
      .withFilter("v1/*/*/*/*")

  for (partition <- exampleGcsFinder.execute(gcStore).partitions) {
    println(s"gcStore $partition")
    println(s"read ${gcStore.read(partition.path)}")
  }


  val exampleDatasets1 = exampleFinder.execute(store)
  val results1 = new Runner()
    .withTestsFor(exampleDatasets1, cases)
    .execute()
  Runner.displayResults(results1)

  val exampleDatasets2 = exampleFinder
    .withCache(exampleDatasets1)
    .execute(store)
  val results2 = new Runner()
    .withTestsFor(exampleDatasets2, cases)
    .withCache(results1)
    .execute()
  Runner.displayResults(results2)

  deleteFiles(tempDir)

  private def createFiles(root: Path, files: Seq[(String, Option[String])]): Unit = {
    files.foreach {
      case (path, contents) =>
        val file = root.resolve(path)
        println(s"creating $file")
        Files.createDirectories(file.getParent)
        Files.write(file, contents.getOrElse("").getBytes)
    }
  }

  private def deleteFiles(root: Path): Unit = {
    Files.walk(root)
      .iterator()
      .asScala
      .toSeq
      .reverse
      .foreach(_.toFile.delete())
  }
}
