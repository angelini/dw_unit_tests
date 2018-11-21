package com.angelini.dw_unit_tests.store

import java.nio.file.Path

trait Store {
  def find(root: Path, datasetFilter: String, partitionFilter: String): Map[Path, Seq[Path]]

  def list(path: Path): Seq[Path]

  def read(path: Path): String
}