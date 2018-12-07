package com.angelini.dw_unit_tests

import java.nio.file.{Path, Paths}
import java.util.concurrent.ForkJoinPool

import com.angelini.dw_unit_tests.store.Store

import scala.collection.parallel.{ForkJoinTaskSupport, ParSeq}

object Finder {
  type SchemaParserFn = (Store, Seq[Path]) => Option[Schema]
}

class Finder(datasetFilter: String = "*",
             partitionFilter: String = "*",
             roots: Seq[Path] = Seq(),
             schemaParser: Finder.SchemaParserFn = (_, _) => None) {
  def copy(datasetFilter: String = datasetFilter,
           partitionFilter: String = partitionFilter,
           roots: Seq[Path] = roots,
           schemaParser: Finder.SchemaParserFn = schemaParser) =
    new Finder(datasetFilter, partitionFilter, roots, schemaParser)

  def withRoots(roots: Path*): Finder = copy(roots = roots)

  def withRootStrings(roots: String*): Finder =
    copy(roots = roots.map(Paths.get(_)))

  def withDatasetFilter(filter: String): Finder = copy(datasetFilter = filter)

  def withPartitionFilter(filter: String): Finder =
    copy(partitionFilter = filter)

  def withSchemaParser(parser: Finder.SchemaParserFn): Finder =
    copy(schemaParser = parser)

  def execute(store: Store, poolSize: Int = defaultPool): Seq[Dataset] = {
    val pool = new ForkJoinPool(poolSize)

    try {
      val parRoots = roots.par
      parRoots.tasksupport = new ForkJoinTaskSupport(pool)

      parRoots.flatMap { root =>
        store
          .find(root, datasetFilter, partitionFilter)
          .map {
            case (dataset, paths) =>
              val partitions = paths.map { path =>
                Partition(path,
                          () => store.list(path).toVector,
                          files => schemaParser(store, files))
              }
              Dataset(dataset, partitions.toVector)
          }
      }.seq

    } finally {
      pool.shutdown()
    }
  }

  private def defaultPool: Int = Runtime.getRuntime.availableProcessors
}
