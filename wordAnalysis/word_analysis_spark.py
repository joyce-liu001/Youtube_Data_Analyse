from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from wordcloud import WordCloud
import numpy as np

spark = SparkSession.builder.appName("YouTubeTrendingAnalysis").getOrCreate()

def read_files_by_country(spark, dir_name, country_code):
    try:
        json_file = os.path.join(dir_name, f"{country_code}_category_id.json")
        csv_file = os.path.join(dir_name, f"{country_code}_youtube_trending_data.csv")

        # Read JSON file
        json_content = spark.read.json(json_file)

        # Read CSV file
        videos = spark.read.option("header", "true").csv(csv_file)

    except FileNotFoundError:
        print(f"Files for {country_code} not found.")

    return json_content, videos

# Set the country code and call the read_files_by_country function
country_code = 'IN'
json_content, videos = read_files_by_country(spark, '/Users/<USERID>/Desktop/Data/archive2023', country_code)

# Display the DataFrames
json_content.show()
videos.show()


videos = videos.withColumn('description', F.when(F.col('description').isNull(), '').otherwise(F.col('description')))

# Convert 'publishedAt' and 'trending_date' columns to datetime
videos = videos.withColumn('publishedAt', F.to_timestamp(videos['publishedAt']))
videos = videos.withColumn('trending_date', F.to_timestamp(videos['trending_date']))

# Drop rows with null values in 'publishedAt' and 'trending_date'
videos = videos.dropna(subset=['publishedAt', 'trending_date'])

# Create new columns for 'publish_date', 'publish_time', and 'trending_date'
videos = videos.withColumn('publish_date', F.to_date(videos['publishedAt']))
videos = videos.withColumn('publish_time', F.date_format(videos['publishedAt'], 'HH:mm:ss'))
videos = videos.withColumn('trending_date', F.to_date(videos['trending_date']))

# Create a new DataFrame from 'json_content'
json_df = json_content.select("items.id", "items.snippet.title").withColumnRenamed("id", "category_id")

# Convert 'category_id' column to int
json_df = json_df.withColumn('category_id', json_df['category_id'].cast('int'))

# Merge the DataFrames
videos = videos.join(json_df, videos['categoryId'] == json_df['category_id'], 'left_outer')
videos = videos.drop('category_id')

# Group by 'video_id' and 'category_title' and count 'Trending Days'
trending_repeat = videos.groupBy('video_id', 'category_title').agg(F.count('category_title').alias('Trending Days'))

# Pivot the table
repeat_all = trending_repeat.groupBy('Trending Days').pivot('category_title').agg(F.count('video_id').alias('count')).na.fill(0)

# Calculate the percentage
repeat_all_percentage = repeat_all.withColumn("Total", sum(repeat_all[col] for col in repeat_all.columns))
repeat_all_percentage = repeat_all_percentage.select(*((F.col(col) / F.col('Total') * 100).alias(col) for col in repeat_all_percentage.columns))


# Extract and count tags
all_tags = merge_videos.select('tags').rdd.flatMap(lambda x: x[0].split('|')).map(lambda x: x.strip(' " ').strip())
tag_counts = Counter(all_tags)

# Convert to DataFrame
top_tags_df = spark.createDataFrame(tag_counts.most_common(20), ['Tag', 'Count'])
top_tags_df = top_tags_df.withColumn('%_count', F.round((F.col('Count') / F.sum('Count').over(), 2) * 100, 2))

# Collect data to the driver for plotting
top_tags_df_pd = top_tags_df.toPandas()

# Plotting
fig, ax = plt.subplots(figsize=(12, 8))

# Set the rotation for the x-axis labels before creating the bar plot
plt.xticks(rotation=45, ha='center')  # Rotate x-axis labels for better visibility

# Create the bar plot
ax.bar(top_tags_df_pd['Tag'], top_tags_df_pd['Count'], color='#845EC2', edgecolor='#FBEAFF', linewidth=1)

# Setting x/y axis line color
ax.spines['left'].set_color('#B39CD0')
ax.spines['bottom'].set_color('#B39CD0')

# Adjusting horizontal lines so that they are underneath bars
ax.set_axisbelow(True)
ax.yaxis.grid(True, color='#B39CD0')

# Set title name and color
ax.set_title(country_code + ' Top 20 most common tags')

# Setting label names and colors
ax.set_xlabel('Tag')
ax.set_ylabel('Number of videos')

# Show the plot
ax.set_facecolor('#FBEAFF')
plt.tight_layout()
plt.show()

# Extract all words from the 'title' column
all_words = merge_videos.select('title').rdd.flatMap(lambda x: x[0].split()).map(lambda x: x.strip().strip())

# Count the occurrences of each word
word_counts = Counter(all_words)

# Convert all the words to lowercase
word_counts_lower = Counter(word.lower() for word in word_counts)

# Remove specified words
to_remove = ['-','|','&','/','-', 'the', 'to', 'in', 'of', 'a', 'by', 'on', 'and']
for word in to_remove:
    word_counts_lower.pop(word, None)

# Convert to DataFrame
top_words_df = spark.createDataFrame(word_counts_lower.most_common(20), ['Word', 'Count'])
top_words_df = top_words_df.withColumn('%_count', F.round((F.col('Count') / F.sum('Count').over(), 2) * 100, 2))

# Remove specified words from DataFrame
top_words_df = top_words_df.filter(~F.col('Word').isin(to_remove))

# Collect data to the driver for plotting
top_words_df_pd = top_words_df.toPandas()

# Generate a word cloud using the word frequencies
wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_counts_lower)

# Create a figure
plt.figure(figsize=(12, 8))

# Display the word cloud
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title('')

# Show the plot
plt.tight_layout()
plt.show()


# Calculate title length and collect data to the driver
title_data = merge_videos.select(F.length('title').alias('title_length')).rdd.flatMap(lambda x: x).collect()

# Create a histogram
fig, ax = plt.subplots(figsize=(8, 6))

# Plot the histogram
ax.hist(title_data, bins=20, color='#845EC2', edgecolor='black')

# Calculate mean and median
mean_value = sum(title_data) / len(title_data)
median_value = sorted(title_data)[len(title_data) // 2]

# Add a dotted line for the mean
ax.axvline(mean_value, color='#DCB0FF', linestyle='dotted', linewidth=2, label='Mean')

# Add a solid line for the median
ax.axvline(median_value, color='#DCB0FF', linestyle='solid', linewidth=2, label='Median')

# Setting label names and colors
ax.set_xlabel('Title Length')
ax.set_ylabel('Frequency')

# Setting x/y axis line color
ax.spines['left'].set_color('#4B4453')
ax.spines['bottom'].set_color('#4B4453')

# Adjusting horizontal lines so that they are underneath bars
ax.set_axisbelow(True)
ax.yaxis.grid(True, color='#4B4453')

# Set background color
ax.set_facecolor('#FFFFFF')
fig.set_facecolor('#FFFFFF')

ax.set_title(country_code + ' Title Length Histogram', color='#4B4453')
plt.legend()


# Stop the Spark session
spark.stop()
