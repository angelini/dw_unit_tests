package com.angelini.dw_unit_tests.store

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystems, Path, Paths}

import com.angelini.dw_unit_tests.utils.Control
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{BlobInfo, Storage, StorageOptions}

import scala.collection.JavaConverters._

class GCStore extends Store with WriteableStore {
  lazy val client: Storage = buildClient()

  override def find(root: Path, filter: String): Seq[Path] = {
    val (bucket, objPath) = splitBucket(root)
    val fullFilter = root.resolve(filter).normalize
    val matcher = FileSystems.getDefault.getPathMatcher(s"glob:$fullFilter")
    listByPrefix(bucket, prefixBeforeGlob(objPath, filter))
      .map(path => path.getRoot.resolve(path.subpath(0, fullFilter.getNameCount)))
      .distinct
      .filter(matcher.matches(_))
  }

  override def list(path: Path): Seq[Path] = {
    val (bucket, objPath) = splitBucket(path)
    listByPrefix(bucket, objPath.toString)
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

  private def splitBucket(path: Path): (String, Path) = {
    val objPath = if (path.getNameCount == 1) {
      Paths.get("")
    } else {
      path.subpath(1, path.getNameCount)
    }
    (path.getName(0).toString, objPath)
  }

  private def listByPrefix(bucket: String, prefix: String): Seq[Path] = {
    val options = BlobListOption.prefix(prefix)
    client.list(bucket, options).iterateAll.asScala
      .map(blob => Paths.get(s"/$bucket/${blob.getName}"))
      .toSeq
  }

  private def prefixBeforeGlob(objPath: Path, filter: String): String = {
    val globIdx = filter.indexOf('*')
    objPath.resolve(filter.slice(0, globIdx)).toString
  }

  private def buildClient(): Storage = StorageOptions.getDefaultInstance.getService
}
