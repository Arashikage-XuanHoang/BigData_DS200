from pyspark import SparkContext
import csv

sc = SparkContext()

# ==============================
# 1. MovieID → List of Genres
# ==============================
movies = sc.textFile("hdfs:///input/movies.txt") \
    .map(lambda x: next(csv.reader([x]))) \
    .map(lambda x: (x[0], x[2].split("|")))  
# (MovieID, [Genre1, Genre2, ...])

# ==============================
# 2. Ratings
# ==============================
ratings1 = sc.textFile("hdfs:///input/ratings_1.txt") \
    .map(lambda x: x.split(",")) \
    .map(lambda x: (x[1], float(x[2])))

ratings2 = sc.textFile("hdfs:///input/ratings_2.txt") \
    .map(lambda x: x.split(",")) \
    .map(lambda x: (x[1], float(x[2])))

ratings = ratings1.union(ratings2)
# (MovieID, Rating)

# ==============================
# 3. Join → (MovieID, (Genres, Rating))
# ==============================
movie_rating = movies.join(ratings)

# ==============================
# 4. Flatten → (Genre, Rating)
# ==============================
genre_rating = movie_rating.flatMap(
    lambda x: [(genre, x[1][1]) for genre in x[1][0]]
)

# ==============================
# 5. Tính tổng + count
# ==============================
genre_agg = genre_rating.mapValues(lambda r: (r, 1)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# ==============================
# 6. Tính average
# ==============================
genre_avg = genre_agg.mapValues(
    lambda x: x[0] / x[1]
)

# ==============================
# 7. Sort theo điểm giảm dần
# ==============================
result = genre_avg.sortBy(lambda x: -x[1])

# ==============================
# 8. In kết quả
# ==============================
print("\nAverage rating by Genre:")
for g in result.collect():
    print(g)

# ==============================
# 9. Lưu ra HDFS 
# ==============================
output = result.map(lambda x: f"{x[0]},{x[1]:.2f}")
output.coalesce(1).saveAsTextFile("hdfs:///output/genre_avg")