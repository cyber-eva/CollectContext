import org.apache.spark.sql
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.SparkSession

object CollectContext {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder.appName("ChainBuilder").getOrCreate()
    import spark.implicits._

    val input_folder  = args(0)
    val output_folder = args(1)

    val data = spark.read.
      format("parquet").
      option("inferSchema","false").
      option("mergeSchema","true").
      load(input_folder)

    val data_work = data.select(
      $"utm_source".cast(sql.types.StringType),
    $"utm_medium".cast(sql.types.StringType),
    $"utm_campaign".cast(sql.types.StringType),
    $"utm_content".cast(sql.types.StringType),
    $"utm_term".cast(sql.types.StringType),
    $"profile_id".cast(sql.types.StringType),
    $"ad_id".cast(sql.types.StringType),
    $"creative_id".cast(sql.types.StringType),
    $"HitTimeStamp".cast(sql.types.LongType),
    $"interaction_type".cast(sql.types.StringType)
    )

    val data_date = data_work.withColumn("date",from_unixtime($"HitTimeStamp"/1000,"yyyy-MM-dd:HH"))

    val data_interaction = data_date.
      withColumn("views",when($"interaction_type" === "view",1).otherwise(0)).
      withColumn("clicks",when($"interaction_type" === "click",1).otherwise(0)).
      withColumn("impressions",when($"interaction_type" === "impression" or $"interaction_type" === "session",1).otherwise(0))

    val data_agg = data_interaction.
      groupBy(
        $"utm_source",
        $"utm_medium",
        $"utm_campaign",
        $"utm_content",
        $"utm_term",
        $"profile_id",
        $"ad_id",
        $"creative_id",
        $"date"
      ).agg(
      sum($"views").as("views"),
      sum($"clicks").as("clicks"),
      sum($"impressions").as("impressions")
    )

    val data_hts = data_agg.withColumn("HitTimeStamp",unix_timestamp($"date","yyy-MM-dd:HH"))

    val data_result = data_hts.select(
      $"utm_source",
      $"utm_medium",
      $"utm_campaign",
      $"utm_content",
      $"utm_term",
      $"profile_id",
      $"ad_id",
      $"creative_id",
      $"HitTimeStamp",
      $"views",
      $"clicks",
      $"impressions",
      $"sessions"
    )

    data_result.
      write.format("csv").
      option("header","true").
      mode("overwrite").
      save(output_folder)



  }

}
