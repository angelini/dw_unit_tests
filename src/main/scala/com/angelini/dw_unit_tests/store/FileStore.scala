package com.angelini.dw_unit_tests.store

import java.nio.file.{FileSystems, Files, Path}

import com.angelini.dw_unit_tests.utils.Control

import scala.collection.JavaConverters._
import scala.io.Source

class FileStore extends Store {
  override def find(root: Path, filter: String): Seq[Path] = {
    val matcher = FileSystems.getDefault
      .getPathMatcher(s"glob:${root.resolve(filter).normalize}")
    Files.walk(root).iterator().asScala.filter(matcher.matches).toSeq
  }

  override def list(path: Path): Seq[Path] =
    Files.list(path).iterator().asScala.toSeq

  override def read(path: Path): String = Control.using(Source.fromFile(path.toFile)) { file =>
    file.getLines.mkString
  }
}
