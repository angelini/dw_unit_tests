package com.angelini.dw_unit_tests

import java.io.StringWriter

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, JsonScalaEnumeration}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object ColumnType extends Enumeration {
  val IntT, FloatT, StringT, BooleanT = Value
}

class ColumnType extends TypeReference[ColumnType.type]

case class Column(@JsonProperty("name") name: String,
                  @JsonProperty("type") @JsonScalaEnumeration(classOf[ColumnType]) col_type: ColumnType.Value,
                  @JsonProperty("nullable") nullable: Boolean = false)

case class Schema(@JsonProperty("columns") columns: Seq[Column]) {
  def toJSON: String = Schema.toJSON(this)
}

object Schema {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def fromJSON(json: String): Schema = {
    mapper.readValue[Schema](json)
  }

  def toJSON(schema: Schema): String = {
    val writer = new StringWriter()
    mapper.writeValue(writer, schema)
    writer.toString
  }
}
