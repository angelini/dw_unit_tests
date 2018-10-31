package com.angelini.dw_unit_tests.store

import java.nio.file.Path

class GCStore extends Store {
  override def find(root: Path, filter: String): Seq[Path] = ???

  override def list(path: Path): Seq[Path] = ???

  override def read(path: Path): String = ???
}
