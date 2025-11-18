package dbis.stark.sql

import dbis.stark.STObject
import dbis.stark.sql.raster._
import dbis.stark.sql.spatial._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object Functions {

  spatial.registerUDTs()
  raster.registerUDTs()

  val fromWKT = udf(STObject.fromWKT _)

  def register(implicit spark: SparkSession): Unit = {

    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_wkt", STAsWKT, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_geomfromwkt", STGeomFromWKT, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_geomfromtile", STGeomFromTile, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_point", STPoint, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_sto", MakeSTObject, "scala_udf")


    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_contains", STContains, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_containedby", STContainedBy, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("st_intersects", STIntersects, "scala_udf")

    //Select-Getters
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("ulx", GetUlx, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("uly", GetUly, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("width", GetWidth, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("height", GetHeight, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("data", GetData, "scala_udf")

    spark.sessionState.functionRegistry.createOrReplaceTempFunction("r_max", TileMax, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("r_min", TileMin, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("r_median", TileMedian, "scala_udf")

    // Histogram functions
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("histogram", CalcTileHistogram, "scala_udf")
    spark.udf.register("rasterHistogram", new CalcRasterHistogram)
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("h_value", HistogramValue, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("h_lower", HistogramLowerBounds, "scala_udf")
    spark.sessionState.functionRegistry.createOrReplaceTempFunction("h_upper", HistogramUpperBounds, "scala_udf")
  }
}
