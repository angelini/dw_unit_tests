package com.angelini.dw_unit_tests.store

import java.nio.ByteBuffer
import java.nio.file.Path

trait WriteableStore {
  def createDirectory(path: Path): Unit

  def write(path: Path, bytes: ByteBuffer): Unit
}
