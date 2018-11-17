package com.angelini.dw_unit_tests

import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}

import com.angelini.dw_unit_tests.sampler.RandomSampler
import com.angelini.dw_unit_tests.store.{FileStore, GCStore, WriteableStore}

import scala.collection.JavaConverters._

class HasMetadata extends PartitionTestCase {
  override def run(partition: Partition): TestExecution.PartitionResult =
    partition.files.find(_.getFileName.endsWith("metadata")) match {
      case Some(_) => TestExecution.Success()
      case None => TestExecution.Failure("Metadata not found")
    }
}

object Main extends App {
  val Remote: Boolean = false
  println(s"remote: $Remote")

  if (Remote) {
    println(s"service_account: ${sys.env("GOOGLE_APPLICATION_CREDENTIALS")}")
  }

  val store = if (!Remote) new FileStore else new GCStore
  val cases = Seq(new HasMetadata)

  val schema = Schema(Seq(
    Column("id", ColumnType.IntT),
    Column("value", ColumnType.StringT)
  ))

  var tempDir = Files.createTempDirectory("")
  if (Remote) {
    tempDir = Paths.get("/test-list-version-files").resolve(
      Paths.get("/").relativize(tempDir)
    )
  }

  createFiles(store, tempDir, Seq(
    ("data/2018/01/01/metadata", Some("123")),
    ("data/2018/01/01/schema", Some(schema.toJSON)),
    ("data/2018/01/02/metadata", Some("456")),
    ("data/2018/01/02/schema", Some(schema.toJSON)),
    ("data/2018/01/03/other", None)
  ))

  val exampleFinder = new Finder(tempDir)
    .withFilter("data/*/*/*")
    .withSchemaParser((store, filePaths) => {
      filePaths.find(_.getFileName.endsWith("schema")).map(path => {
        Schema.fromJSON(store.read(path))
      })
    })

  for (partition <- exampleFinder.execute(store).partitions) {
    println(s"partition: ${partition.path}")
  }

  println("")

  val exampleDatasets = exampleFinder.execute(store)

  val results1 = new Runner()
    .withTestsFor(exampleDatasets, cases)
    .execute()
  Runner.displayResults(results1)

  val results2 = new Runner()
    .withTestsFor(exampleDatasets, cases)
    .withSampler(new RandomSampler(1, 0.5))
    .withCache(results1)
    .execute()
  Runner.displayResults(results2)

  if (!Remote) {
    deleteFiles(tempDir)
  }

  private def createFiles(store: WriteableStore,
                          root: Path,
                          files: Seq[(String, Option[String])]): Unit = {
    files.foreach {
      case (path, contents) =>
        val file = root.resolve(path)
        store.createDirectory(file.getParent)
        store.write(file, ByteBuffer.wrap(contents.getOrElse("").getBytes))
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
