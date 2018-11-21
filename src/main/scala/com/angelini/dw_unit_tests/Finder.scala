package com.angelini.dw_unit_tests

import java.nio.file.{Path, Paths}

import com.angelini.dw_unit_tests.store.Store

object Finder {
  type SchemaParserFn = (Store, Seq[Path]) => Option[Schema]
}

class Finder(root: Path,
             datasetFilter: String = "*",
             partitionFilter: String = "*",
             schemaParser: Finder.SchemaParserFn = (_, _) => None) {

  def copy(root: Path = root,
           datasetFilter: String = datasetFilter,
           partitionFilter: String = partitionFilter,
           schemaParser: Finder.SchemaParserFn = schemaParser) =
    new Finder(root, datasetFilter, partitionFilter, schemaParser)

  def withDatasetFilter(filter: String): Finder = copy(datasetFilter = filter)

  def withPartitionFilter(filter: String): Finder = copy(partitionFilter = filter)

  def withSchemaParser(parser: Finder.SchemaParserFn): Finder = copy(schemaParser = parser)

  def execute(store: Store): Seq[Dataset] = {
    store.find(root, datasetFilter, partitionFilter)
      .map{ case (dataset, partitions) =>
        Dataset(dataset, partitions.map(path => {
          Partition(
            path,
            () => store.list(path).toVector,
            (files) => schemaParser(store, files)
          )
        }).toVector)
      }
      .toSeq
  }
}
