from pyspark import SparkContext
import csv

sc = SparkContext()

# ==============================
# 1. Read movies (MovieID → Title)
# ==============================
movies = sc.textFile("hdfs:///input/movies.txt") \
    .map(lambda x: next(csv.reader([x]))) \
    .map(lambda x: (x[0], x[1]))   # (MovieID, Title)

# ==============================
# 2. Read ratings (MovieID → (rating, 1))
# ==============================
ratings1 = sc.textFile("hdfs:///input/ratings_1.txt") \
    .map(lambda x: x.split(",")) \
    .map(lambda x: (x[1], (float(x[2]), 1)))

ratings2 = sc.textFile("hdfs:///input/ratings_2.txt") \
    .map(lambda x: x.split(",")) \
    .map(lambda x: (x[1], (float(x[2]), 1)))

# Gộp 2 file ratings
ratings = ratings1.union(ratings2)

# ==============================
# 3. Tính tổng rating và count
# ==============================
ratings_agg = ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# ==============================
# 4. Tính average + filter >= 5 lượt (theo đề bạn ghi)
# ==============================
ratings_avg = ratings_agg.mapValues(
    lambda x: (x[0] / x[1], x[1])
).filter(lambda x: x[1][1] >= 5)

# ==============================
# 5. Join với movies để lấy title
# ==============================
movie_ratings = movies.join(ratings_avg)
# (MovieID, (Title, (avg, count)))

# ==============================
# 6. Tìm phim có rating cao nhất
# ==============================
top_movie = movie_ratings.map(
    lambda x: (x[1][1][0], x[1][0], x[1][1][1])
).sortBy(lambda x: -x[0]).take(1)

print("Top movie:", top_movie)

# ==============================
# In top 10
# ==============================
top10 = movie_ratings.map(
    lambda x: (x[1][1][0], x[1][0], x[1][1][1])
).sortBy(lambda x: -x[0]).take(10)

print("Top 10 movies:")
for m in top10:
    print(m)

sc.parallelize(top10).saveAsTextFile("hdfs:///output/top10_movies")