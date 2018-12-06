package com.angelini.dw_unit_tests.store

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystems, Path, Paths}

import com.angelini.dw_unit_tests.utils.Control
import com.google.cloud.storage.Storage.{BlobField, BlobListOption}
import com.google.cloud.storage.{BlobInfo, Storage, StorageOptions}

import scala.collection.JavaConverters._

class GCStore extends Store with WriteableStore {
  lazy val client: Storage = buildClient()

  override def find(root: Path,
                    datasetFilter: String,
                    partitionFilter: String): Map[Path, Seq[Path]] = {
    val datasetPath = root.resolve(datasetFilter)
    val partitionPath = datasetPath.resolve(partitionFilter).normalize

    val partitionMatcher = FileSystems.getDefault.getPathMatcher(
      s"glob:$partitionPath"
    )

    listWithWildcards(partitionPath)
      .filter(partitionMatcher.matches)
      .toSeq
      .groupBy(p => p.getRoot.resolve(p.subpath(0, datasetPath.getNameCount)))
  }

  override def list(path: Path): Seq[Path] = {
    val (bucket, objPath) = splitBucket(path)
    listByPrefix(bucket, objPath.toString).toSeq
  }

  override def read(path: Path): String = {
    val (bucket, objPath) = splitBucket(path)
    new String(client.readAllBytes(bucket, objPath.toString), StandardCharsets.UTF_8)
  }

  override def createDirectory(path: Path): Unit = Unit

  override def write(path: Path, bytes: ByteBuffer): Unit = {
    val (bucket, objPath) = splitBucket(path)
    val info = BlobInfo.newBuilder(bucket, objPath.toString)
      .setContentEncoding(StandardCharsets.UTF_8.displayName)
      .build
    Control.using(client.writer(info)) { writer =>
      writer.write(bytes)
    }
  }

  private def listWithWildcards(path: Path): Iterable[Path] = {
    val (bucket, objPath) = splitBucket(path)

    val firstGlob = firstGlobIndex(objPath)
    if (firstGlob == -1) {
      return Seq(path)
    }

    val rest = if (firstGlob == objPath.getNameCount - 1) {
      Paths.get("")
    } else {
      objPath.subpath(firstGlob + 1, objPath.getNameCount)
    }

    listByPrefix(bucket, objPath.subpath(0, firstGlob).toString)
      .par
      .flatMap(p => listWithWildcards(p.resolve(rest)))
      .seq
  }

  private def listByPrefix(bucket: String, prefix: String): Iterable[Path] = {
    client.list(
      bucket,
      BlobListOption.prefix(s"$prefix/"),
      BlobListOption.currentDirectory(),
      BlobListOption.fields(BlobField.NAME)
    ).iterateAll.asScala
      .filter(_.getName != s"$prefix/")
      .map(blob => Paths.get(s"/$bucket/${blob.getName}"))
  }

  private def firstGlobIndex(path: Path): Int = {
    for (i <- 0 until path.getNameCount) {
      if (path.getName(i).toString.contains("*")) {
        return i
      }
    }
    -1
  }

  private def splitBucket(path: Path): (String, Path) = {
    val objPath = if (path.getNameCount == 1) {
      Paths.get("")
    } else {
      path.subpath(1, path.getNameCount)
    }
    (path.getName(0).toString, objPath)
  }

  private def buildClient(): Storage = StorageOptions.getDefaultInstance.getService
}
