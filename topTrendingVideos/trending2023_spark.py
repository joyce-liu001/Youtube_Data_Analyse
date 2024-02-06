from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("TrendingVideos").getOrCreate()


country_code = 'RU'
videos = spark.read.csv("/Users/{UserName}/Desktop/Data/csv_file.csv", header=True)

videos = videos.withColumn("publishedAt", F.to_timestamp("publishedAt"))
videos = videos.withColumn("trending_date", F.to_date("trending_date"))

videos = videos.withColumn("publish_date", videos["publishedAt"].cast("date"))
videos = videos.withColumn("publish_time", F.date_format("publishedAt", "HH:mm:ss"))
videos = videos.withColumn("trending_year", F.year("trending_date"))

windowSpec = Window.partitionBy("video_id")
videos = videos.withColumn("days_trending", F.count("video_id").over(windowSpec))

top_trending = videos.orderBy("days_trending", ascending=False).limit(10)

top_trending.write.mode("overwrite").csv("/Users/{UserName}/Desktop/Data/top_trending")

spark = SparkSession.builder.appName("JsonProcessing").getOrCreate()

json_df = spark.createDataFrame(json_content['items'])

json_df = json_df.withColumn("id", json_df["id"].cast("long"))
json_df = json_df.withColumn("category_title", F.expr("snippet.title"))

videos = videos.join(json_df.select("id", "category_title"), videos["categoryId"] == json_df["id"], "left").drop("id")

windowSpec = Window.partitionBy("video_id", "category_title")
trending_repeat = videos.withColumn("Trending Days", F.count("video_id").over(windowSpec)).groupBy("video_id", "category_title").agg(F.max("Trending Days").alias("Trending Days"))

repeat_all = trending_repeat.groupBy("Trending Days").pivot("category_title").count().na.fill(0)

repeat_all_percentage = repeat_all.select("Trending Days", *((F.col(col) / F.sum(col).over(Window.partitionBy())).alias(col) * 100).alias(col) for col in repeat_all.columns[1:])

json_df.show()
videos.show()
trending_repeat.show()
repeat_all.show()
repeat_all_percentage.show()

sort_trending = videos.groupBy("video_id").count().withColumnRenamed("count", "days_trending")
video_noduplicates = videos.dropDuplicates(["video_id"])
merge_videos = video_noduplicates.join(sort_trending, "video_id", "inner")

sort_trending.show()
video_noduplicates.show()
merge_videos.show()

new_order = ['country', 'trending_year', 'days_trending', 'title', 'category_title',
             'video_id', 'publish_date', 'trending_date', 'channelTitle', 'categoryId',
             'publish_time', 'tags', 'view_count', 'likes', 'dislikes', 'comment_count',
             'thumbnail_link', 'comments_disabled', 'ratings_disabled',
             'description']

top_trending = top_trending.select(*new_order)

top_trending_short = top_trending.select('country', 'trending_year', 'title', 'channelTitle', 'days_trending')
top_trending_short.show()

top_trending.write.mode("append").csv("csv2023/2023top_trending")

top_category = merge_videos.groupBy("channelTitle").count().withColumnRenamed("count", "number of videos")

channel_category = merge_videos.groupBy("channelTitle").agg(F.collect_set("category_title").alias("category_title"))
channel_category = channel_category.withColumn("category_title", F.concat_ws(", ", "category_title"))

result = top_category.join(channel_category, "channelTitle", "left_outer").limit(10)

result = result.withColumn("country", F.lit(country_code))

channel_order = ["country", "channelTitle", "number of videos", "category_title"]
result = result.select(*channel_order)

result.write.mode("append").csv("csv2023/2023top_channel")


spark.stop()
