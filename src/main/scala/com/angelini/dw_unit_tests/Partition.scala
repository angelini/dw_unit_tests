package com.angelini.dw_unit_tests

import java.nio.file.Path

object Partition {
  type LoadFilesFn = () => Vector[Path]

  type LoadSchemaFn = (Vector[Path]) => Option[Schema]
}

case class Partition(path: Path,
                     loadFiles: Partition.LoadFilesFn,
                     loadSchema: Partition.LoadSchemaFn) {

  lazy val files: Vector[Path] = loadFiles()

  lazy val schema: Option[Schema] = loadSchema(files)
}
