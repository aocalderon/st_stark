package puj

import dbis.stark._
import org.apache.spark.SpatialRDD._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

import dbis.stark.spatial.partitioner._

import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel, Coordinate}

import scala.io.Source
import scala.util.Random

import java.io._

object Main {
    
    def main(args: Array[String]): Unit = {
        val filePath = "/opt/Datasets/WKT/points/random_1K.wkt"
        val tolerance = 1e-3
        implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(1 / tolerance))

        val spark = SparkSession
            .builder()
            .master("local[3]")
            .appName("STARK")
            .getOrCreate()
        dbis.stark.sql.Functions.register(spark)

        val sc = spark.sparkContext

        println(s"It works ${tolerance}")
        try {
            /*
            val buffer = Source.fromFile(filePath)

            val points = buffer.getLines().map{ line =>
                val arr = line.split(";")
                val wkt = arr(0)
                val oid = arr(1).toInt
                val tid = Random.nextInt(10)
                (STObject(wkt, tid), oid) 
            }.toList

            println(points.size)
            */

            val n: Int = 10000
            val sd: Double = 250.0
            val x_limit: Double = 1000.0
            val y_limit: Double = 1000.0
            val t_limit: Int = 10
            val Xs = gaussianSeries(n, n / 2.0, sd)
            val Ys = gaussianSeries(n, n / 2.0, sd)
            val points = Xs.zip(Ys).zipWithIndex.map{ case(tuple, oid) =>
                    val t = Random.nextInt(t_limit)
                    val point = G.createPoint(new Coordinate(tuple._1, tuple._2))
                    (STObject(point.toText(), t), oid)
                }
            val pointsRDD = sc.parallelize(points)

            printToFile("/tmp/P.wkt") { p =>
                points.map{ row =>  
                    val sto = row._1
                    val oid = row._2
                    val wkt = sto.wkt
                    val tid = sto.getTemp.get.start.value
                    val x = sto.getGeo.getCoordinate.x
                    val y = sto.getGeo.getCoordinate.y
                    
                    s"$wkt\t$x\t$y\t$tid\t$oid"
                }.foreach(p.println)
            }
           
            val STPartitioner = SpatioTempPartitioner(pointsRDD, cellSize = 100.0, spatial_capacity = 100, temporal_capacity = 20)
            println(s"ST partitions: ${STPartitioner.numPartitions}")
            STPartitioner.printPartitions("/tmp/grid.wkt")
            STPartitioner.savePartitionsById("/tmp/st3d.wkt")

            val partitionedPoints: RDD[(STObject, Int)] = pointsRDD.partitionBy(STPartitioner)
            val P: Array[String] = partitionedPoints.mapPartitionsWithIndex{ (idx, points) =>
                points.map{ case(point, _) =>
                        val tid = point.getStart.get.value
                        val wkt = point.wkt
                        s"$wkt\t$tid\t$idx"
                    }
            }.collect()
            printToFile("/tmp/PP.wkt") { p =>
                P.foreach(p.println)
            }
            

            //buffer.close()
        } catch {
            case e: java.io.FileNotFoundException =>
                println(s"Error: File not found at $filePath")
            case e: Exception =>
                println(s"An error occurred: ${e.getMessage}")
        }
    }

    // Utility function to handle closing the writer automatically
    def printToFile(filename: String)(op: PrintWriter => Unit): Unit = {
        val f = new File(filename)
        val p = new PrintWriter(f)
        try {
            op(p)
        } finally {
            p.close() // Ensures the writer is closed
        }
    }
    
    /**
     * Generates a series of pseudo-random real numbers following a
     * Normal (Gaussian) distribution.
     *
     * The generated values will be clustered around the specified mean (mu),
     * with the degree of spread determined by the standard deviation (sigma).
     *
     * @param size The number of elements (Double values) to generate in the list.
     * Defaults to 1000.
     * @param mean The arithmetic mean (mu) of the distribution, which acts as the
     * center point. Defaults to 500.0.
     * @param stdDev The standard deviation (sigma). Controls the spread or
     * dispersion of the data. A larger value results in a wider spread.
     * Defaults to 150.0.
     * @return A List containing 'size' double-precision floating-point numbers
     * distributed according to the specified Gaussian parameters.
     */
    def gaussianSeries(size: Int = 1000, mean: Double = 500.0, stdDev: Double = 150): List[Double] = List.fill(size) {
        // nextGaussian() generates numbers with mean=0.0 and stdDev=1.0
        // Scale and shift the result
        mean + stdDev * Random.nextGaussian()
    }
}
