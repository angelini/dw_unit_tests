package com.angelini.dw_unit_tests

import java.nio.file.{Path, Paths}

import com.angelini.dw_unit_tests.store.Store

object Finder {
  type ParseSchemaFn = (Store, Seq[Path]) => Option[Schema]
}

class Finder(root: Path,
             filter: String = "*",
             parseSchema: Finder.ParseSchemaFn = (_, _) => None) {

  def copy(root: Path = root,
           filter: String = filter,
           parseSchema: Finder.ParseSchemaFn = parseSchema) =
    new Finder(root, filter, parseSchema)

  def withFilter(pattern: String): Finder = copy(filter = pattern)

  def withSchemaParser(parser: Finder.ParseSchemaFn): Finder = copy(parseSchema = parser)

  def execute(store: Store): Dataset = {
    Dataset(root, store.find(root, filter).map(path => {
      Partition(path, () => store.list(path).toVector, (files) => parseSchema(store, files))
    }).toVector)
  }
}
