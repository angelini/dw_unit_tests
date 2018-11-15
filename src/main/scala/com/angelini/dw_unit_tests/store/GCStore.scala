package com.angelini.dw_unit_tests.store

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystems, Path, Paths}

import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{BlobId, Storage, StorageOptions}

import scala.collection.JavaConverters._

class GCStore(bucket: String) extends Store {
  lazy val client: Storage = buildClient()

  override def find(root: Path, filter: String): Seq[Path] = {
    val matcher = FileSystems.getDefault
      .getPathMatcher(s"glob:${root.resolve(filter).normalize}")
    listByPrefix(prefixBeforeGlob(filter))
      .filter(matcher.matches(_))
  }

  override def list(path: Path): Seq[Path] = {
    listByPrefix(path.toString)
  }

  override def read(path: Path): String = {
    val blob = client.get(BlobId.of(bucket, path.toString))
    val buffer = ByteBuffer.allocate(blob.getSize.toInt)
    blob.reader().read(buffer)
    new String(buffer.array, StandardCharsets.UTF_8)
  }

  private def listByPrefix(prefix: String): Seq[Path] = {
    val options = BlobListOption.prefix(prefix)
    client.list(bucket, options).iterateAll.asScala
      .map(blob => Paths.get(blob.getName))
      .toSeq
  }

  private def prefixBeforeGlob(filter: String): String = {
    val globIdx = filter.indexOf('*')
    filter.slice(0, globIdx)
  }

  private def buildClient(): Storage = {
    StorageOptions.getDefaultInstance.getService
  }
}
