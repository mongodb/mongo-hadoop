package com.mongodb.spark.pickle

import org.apache.spark.api.python.Converter


class NoopConverter extends Converter[Any, Any] {
  override def convert(obj: Any): Any = { obj }
}
