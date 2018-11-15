package com.angelini.dw_unit_tests

import java.nio.file.{Path, Paths}

import com.angelini.dw_unit_tests.store.Store

object Finder {
  type ParseSchemaFn = (Store, Seq[Path]) => Option[Schema]
}

class Finder(root: Path,
             filter: String = "*",
             parseSchema: Finder.ParseSchemaFn = (_, _) => None,
             cache: Dataset = Dataset(Paths.get("/"), Vector())) {

  def copy(root: Path = root,
           filter: String = filter,
           parseSchema: Finder.ParseSchemaFn = parseSchema,
           cache: Dataset = cache) =
    new Finder(root, filter, parseSchema, cache)

  def withFilter(pattern: String): Finder = copy(filter = pattern)

  def withSchemaParser(parser: Finder.ParseSchemaFn): Finder = copy(parseSchema = parser)

  def withCache(cache: Dataset): Finder = copy(cache = cache)

  def execute(store: Store): Dataset = {
    Dataset(root, store.find(root, filter).map(path => {
      val files = cache.partitionByPath(path).map(_.files).getOrElse(store.list(path).toVector)
      Partition(path, parseSchema(store, files), files)
    }).toVector)
  }
}
