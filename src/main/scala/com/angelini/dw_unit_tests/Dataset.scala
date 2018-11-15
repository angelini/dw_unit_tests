package com.angelini.dw_unit_tests

import java.nio.file.Path

case class Partition(path: Path, schema: Option[Schema], files: Vector[Path])

case class Dataset(root: Path, partitions: Vector[Partition]) {
  def partitionByPath(path: Path): Option[Partition] =
    partitions.find(_.path == Dataset.removeRoot(root, path))
}

object Dataset {
  def removeRoot(root: Path, path: Path): Path =
    if (path.startsWith(root))
      path.subpath(root.getNameCount, path.getNameCount)
    else path
}
