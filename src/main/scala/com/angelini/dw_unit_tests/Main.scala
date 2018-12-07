package com.angelini.dw_unit_tests

import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}

import com.angelini.dw_unit_tests.sampler.RandomSampler
import com.angelini.dw_unit_tests.store.{FileStore, GCStore, Store, WriteableStore}

import scala.collection.JavaConverters._

class HasMetadata extends PartitionTestCase {
  override def run(store: Store, dataset: Dataset, partition: Partition): TestExecution.Result =
    partition.files.find(_.getFileName.endsWith("metadata")) match {
      case Some(_) => TestExecution.Success()
      case None => TestExecution.Failure("Metadata not found")
    }
}

class HasTwoPartitions extends DatasetTestCase {
  override def run(store: Store, dataset: Dataset): TestExecution.Result =
    dataset.partitions.length match {
      case 2 => TestExecution.Success()
      case len if len < 2 => TestExecution.Failure(s"$len is less than 2 partitions")
      case len if len > 2 => TestExecution.Failure(s"$len is more than 2 partitions")
    }
}

object Main extends App {
  val Remote: Boolean = false
  println(s"remote: $Remote")

  if (Remote) {
    println(s"service_account: ${sys.env("GOOGLE_APPLICATION_CREDENTIALS")}")
  }

  val store = if (!Remote) new FileStore else new GCStore
  val cases = Seq(new HasMetadata, new HasTwoPartitions)

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
    ("data/2018/01/03/other", None),
    ("other/2017/02/01/metadata", Some("123")),
    ("other/2017/02/02/metadata", Some("123"))
  ))

  val exampleFinder = new Finder("*", "*/*/*")
    .withSchemaParser((store, filePaths) => {
      filePaths.find(_.getFileName.endsWith("schema")).map(path => {
        Schema.fromJSON(store.read(path))
      })
    })
    .withRoots(tempDir)

  val exampleDatasets = exampleFinder.execute(store)

  for (dataset <- exampleDatasets) {
    for (partition <- dataset.partitions) {
      println(s"d: ${dataset.root}, p: ${partition.path}")
    }
  }
  println("")

  val results = new Runner()
    .withTestsFor(exampleDatasets, cases)
    .withSampler(new RandomSampler(1, 1))
    .execute(store)
  Runner.displayResults(results)

  if (!Remote) {
    deleteFiles(tempDir)
  }

  private def createFiles(store: WriteableStore,
                          root: Path,
                          files: Seq[(String, Option[String])]): Unit = {
    files.par.foreach {
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
