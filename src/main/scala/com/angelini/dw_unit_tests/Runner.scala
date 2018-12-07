package com.angelini.dw_unit_tests

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicInteger

import com.angelini.dw_unit_tests.sampler.{AlwaysSampler, Sampler}
import com.angelini.dw_unit_tests.store.Store

import scala.collection.parallel.ForkJoinTaskSupport

object Runner {
  type Results = Map[Dataset, Map[TestCase, TestExecution.Result]]

  def displayResults(results: Results): Unit = {
    println("-----")
    results.foreach(d => displayDatasetResults(d._1, d._2))
  }

  private def displayDatasetResults(
      dataset: Dataset,
      results: Map[TestCase, TestExecution.Result]): Unit = {
    val sb = StringBuilder.newBuilder
    sb.append(s"${dataset.root}: ")
    sb.append("✓" * results.count(!_._2.failed))

    for ((test, result) <- results.filter(_._2.failed)) {
      sb.append(s"\n    ${test.getClass.getName}: ❌")
      result match {
        case r: TestExecution.PartitionResults =>
          for ((partition, pResult) <- r.partitionResults if pResult.failed) {
            val relative = partition.path.subpath(
              dataset.root.getNameCount,
              partition.path.getNameCount
            )
            sb.append(s"\n        $relative $pResult")
          }
        case r: TestExecution.Result =>
          sb.append(s"\n        $r")
      }
    }

    println(sb)
  }
}

class Runner(cases: Map[Dataset, Seq[TestCase]] = Map(),
             sampler: Sampler = AlwaysSampler) {

  def copy(cases: Map[Dataset, Seq[TestCase]] = cases,
           sampler: Sampler = sampler) =
    new Runner(cases, sampler)

  def withTestsFor(datasets: Seq[Dataset], caseSeq: Seq[TestCase]): Runner = {
    val merged = cases ++ datasets.map(_ -> caseSeq)
    copy(cases = merged.groupBy(_._1).mapValues(_.flatMap(_._2).toSeq.distinct))
  }

  def withSampler(sampler: Sampler): Runner = copy(sampler = sampler)

  def caseCount: Int =
    cases.foldLeft(0)((count, tests) => count + tests._2.length)

  def execute(store: Store, poolSize: Int = defaultPool): Runner.Results = {
    val remaining = new AtomicInteger(caseCount)
    val active = new AtomicInteger(0)
    val pool = new ForkJoinPool(poolSize)

    val statusThread = new Thread {
      override def run(): Unit = {
        while (true) {
          if (remaining.get <= 0) {
            return
          }

          val progress = Math.floor((caseCount - remaining.get).toDouble / (caseCount.toDouble / 20)).toInt
          val sb = StringBuilder.newBuilder
          sb.append(s"active: ${active.get} [")
          sb.append("=" * progress)
          sb.append(" " * (20 - progress))
          sb.append("]")
          println(sb)

          Thread.sleep(2000)
        }
      }
    }
    statusThread.start()

    println(s"Execution $caseCount cases across ${cases.size} datasets")

    try {
      val parCases = cases.filter {
        case (dataset, _) => sampler.includeDataset(dataset)
      }.par
      parCases.tasksupport = new ForkJoinTaskSupport(pool)

      parCases.map {
        case (dataset, tests) =>
          val parTests = tests.par
          parTests.tasksupport = new ForkJoinTaskSupport(pool)

          val results = parTests
            .map { test =>
              active.incrementAndGet()
              val result = executeTestCase(store, test, dataset, pool)
              active.decrementAndGet()
              remaining.decrementAndGet()
              (test, result)
            }
            .seq
            .toMap
          (dataset, results)
      }.seq

    } finally {
      pool.shutdown()
    }
  }

  private def executeTestCase(store: Store,
                              test: TestCase,
                              dataset: Dataset,
                              pool: ForkJoinPool): TestExecution.Result =
    test match {
      case test: PartitionTestCase =>
        val parPartitions = dataset.partitions
          .filter(sampler.includePartition(dataset, _))
          .par
        parPartitions.tasksupport = new ForkJoinTaskSupport(pool)

        val partitionResults = parPartitions
          .map { partition =>
            (partition, test.run(store, dataset, partition))
          }
          .seq
          .toMap

        TestExecution.PartitionResults(
          if (partitionResults.values.exists(_.failed))
            TestExecution.Failure("Partition test failure")
          else
            TestExecution.Success(),
          partitionResults
        )

      case test: DatasetTestCase => test.run(store, dataset)
    }

  private def defaultPool: Int = Runtime.getRuntime.availableProcessors
}
