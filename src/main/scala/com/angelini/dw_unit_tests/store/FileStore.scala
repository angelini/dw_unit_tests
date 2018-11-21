package com.angelini.dw_unit_tests.store

import java.nio.ByteBuffer
import java.nio.file.{FileSystems, Files, Path}

import com.angelini.dw_unit_tests.utils.Control

import scala.collection.JavaConverters._
import scala.io.Source

class FileStore extends Store with WriteableStore {
  override def find(root: Path,
                    datasetFilter: String,
                    paritionFilter: String): Map[Path, Seq[Path]] = {
    val datasetPath = root.resolve(datasetFilter)
    val partitionPath = root.resolve(datasetFilter).resolve(paritionFilter).normalize

    val partitionMatcher = FileSystems.getDefault.getPathMatcher(
      s"glob:${partitionPath}"
    )

    Files.walk(root).iterator().asScala
      .filter(partitionMatcher.matches)
      .toSeq
      .groupBy(p => p.getRoot.resolve(p.subpath(0, datasetPath.getNameCount)))
  }

  override def list(path: Path): Seq[Path] =
    Files.list(path).iterator().asScala.toSeq

  override def read(path: Path): String = Control.using(Source.fromFile(path.toFile)) { file =>
    file.getLines.mkString
  }

  override def createDirectory(path: Path): Unit = Files.createDirectories(path)

  override def write(path: Path, bytes: ByteBuffer): Unit = Files.write(path, bytes.array)
}
