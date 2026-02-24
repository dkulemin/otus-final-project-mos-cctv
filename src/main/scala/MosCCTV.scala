import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}
import java.util.Properties

object MosCCTV {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .master("spark://spark-master:7077")
            .appName("MosCCTV")
            .getOrCreate()

        val properties = new Properties()
        properties.setProperty("user","user")
        properties.setProperty("password","password")
        properties.setProperty("driver","org.postgresql.Driver")

        val camerasDF = spark.read.json("/app/data/cameras.json")
        val objectsDF = spark.read.json("/app/data/objects_3_1598.json")

        import spark.implicits._

        val stringToArray = F.udf(
            (x: Array[String]) => x.map(elem => elem.substring(1, elem.length - 1).split(','))
        )

        val prepObjectsDF = objectsDF
            .withColumns(Map(
                "c" -> F.when(
                    $"geoData.type" === "MultiPolygon",
                    stringToArray(F.flatten(F.flatten($"geoData.coordinates")))
                ).otherwise(F.flatten($"geoData.coordinates")),
                "g" -> F.flatten(F.flatten($"geoData.geometries.coordinates"))
            ))
            .select(
                $"UNOM",
                $"ADDRESS",
                $"ADM_AREA",
                F.coalesce($"g", $"c").alias("coordinates")
            )
            .withColumn(
                "center",
                F.aggregate(
                    $"coordinates",
                    F.array(F.lit(0.0), F.lit(0.0), F.lit(0)),
                    (acc, x) => F.array(acc(0) + x(0), acc(1) + x(1), acc(2) + 1)
                )
            )
            .select(
                $"UNOM",
                $"ADDRESS".alias("addr"),
                $"ADM_AREA",
                F.when(
                    !$"center".isNull,
                    F.array($"center"(0) / $"center"(2), $"center"(1) / $"center"(2))
                ).otherwise(null).alias("building_coordinates")
            )

        val objectsWithCCTV = prepObjectsDF.join(camerasDF, "UNOM", "left")
            .select(
                $"UNOM".alias("obj_unom"),
                $"addr",
                $"ADM_AREA".alias("adm_area"),
                $"building_coordinates",
                F.when(!$"geoData.coordinates".isNull, 1).otherwise(0).alias("has_cctv")
            )
            .groupBy("obj_unom", "addr", "adm_area", "building_coordinates", "has_cctv")
            .agg(F.sum("has_cctv").alias("cctv_number"))

        objectsWithCCTV
            .write
            .mode("overwrite")
            .jdbc("jdbc:postgresql://postgres/postgres", "has_cctv", properties)

        val haversineDistance = F.udf(

            /**
              * R = 6371e3; // metres
              * φ1 = lat1 * Math.PI/180; // φ, λ in radians
              * φ2 = lat2 * Math.PI/180;
              * Δφ = (lat2-lat1) * Math.PI/180;
              * Δλ = (lon2-lon1) * Math.PI/180;
            
              * a = Math.sin(Δφ/2) * Math.sin(Δφ/2) +
              *   Math.cos(φ1) * Math.cos(φ2) *
              *    Math.sin(Δλ/2) * Math.sin(Δλ/2);
              * c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
            
              * d = R * c; // in metres
              */

            (x: Array[Double], y: Array[Double]) => {
                val R = 6371e3
                val piValue = scala.math.Pi
                val phi_1 = x(1) * piValue / 180
                val phi_2 = y(1) * piValue / 180
                val delta_phi = (y(1) - x(1)) * piValue / 180
                val delta_lambda = (y(0) - x(0)) * piValue / 180
                val a = scala.math.pow(scala.math.sin(delta_phi / 2), 2) +
                    scala.math.cos(phi_1) * scala.math.cos(phi_2) *
                    scala.math.pow(scala.math.sin(delta_lambda / 2), 2)
                val c = 2 * scala.math.atan2(scala.math.sqrt(a), scala.math.sqrt(1 - a))
                R * c
            }
        )

        val objectsFiltered = objectsWithCCTV
            .filter($"has_cctv" === 0 && !$"building_coordinates".isNull)
        val objectsNumPartitions = objectsFiltered.rdd.getNumPartitions
        val camerasNumPartitions = camerasDF.rdd.getNumPartitions
        objectsFiltered.repartition(objectsNumPartitions)
        camerasDF.repartition(camerasNumPartitions)
        objectsFiltered.crossJoin(camerasDF)
            .select(
                $"obj_unom",
                $"addr",
                $"adm_area",
                haversineDistance($"building_coordinates", $"geoData.coordinates").alias("distance")
            )
            .groupBy(
                "obj_unom",
                "addr",
                "adm_area"
            )
            .agg(F.min("distance").alias("closest_cctv_distance_mt"))
            .write
            .mode("overwrite")
            .jdbc("jdbc:postgresql://postgres/postgres", "closest_cctv", properties)
    }
}