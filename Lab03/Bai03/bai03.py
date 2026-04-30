from pyspark import SparkContext
import csv
import os

sc = SparkContext()

# ==============================
# 1. UserID → Gender
# ==============================
users = sc.textFile("hdfs:///input/users.txt") \
    .map(lambda x: x.split(",")) \
    .map(lambda x: (x[0], x[1]))  
# (UserID, Gender)

# ==============================
# 2. Movies (MovieID → Title)
# ==============================
movies = sc.textFile("hdfs:///input/movies.txt") \
    .map(lambda x: next(csv.reader([x]))) \
    .map(lambda x: (x[0], x[1]))  
# (MovieID, Title)

# ==============================
# 3. Ratings
# ==============================
ratings = sc.textFile("hdfs:///input/ratings_1.txt") \
    .union(sc.textFile("hdfs:///input/ratings_2.txt")) \
    .map(lambda x: x.split(",")) \
    .map(lambda x: (x[0], (x[1], float(x[2]))))
# (UserID, (MovieID, Rating))

# ==============================
# 4. Join ratings + users
# ==============================
user_rating = ratings.join(users)
# (UserID, ((MovieID, Rating), Gender))

# ==============================
# 5. (MovieID, Gender) → Rating
# ==============================
movie_gender_rating = user_rating.map(
    lambda x: ((x[1][0][0], x[1][1]), x[1][0][1])
)
# ((MovieID, Gender), Rating)

# ==============================
# 6. Aggregate
# ==============================
agg = movie_gender_rating.mapValues(lambda r: (r, 1)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

avg = agg.mapValues(lambda x: x[0] / x[1])
# ((MovieID, Gender), Avg)

# ==============================
# 7. Join với movie để lấy title
# ==============================
result = avg.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
    .join(movies)
# (MovieID, ((Gender, Avg), Title))

# ==============================
# 8. Format kết quả
# ==============================
final = result.map(
    lambda x: (x[1][1], x[1][0][0], x[1][0][1])
)
# (Title, Gender, Avg)

# ==============================
# 9. IN KẾT QUẢ
# ==============================
print("\n=== Average Rating by Movie & Gender ===")
for r in final.take(20):
    print(f"{r[0]} | {r[1]} | {r[2]:.2f}")

# ==============================
# 10. LƯU HDFS 
# ==============================
output = final.map(lambda x: f"{x[0]},{x[1]},{x[2]:.2f}")

# xóa output cũ 
os.system("hdfs dfs -rm -r -f /output/movie_gender_avg")

output.coalesce(1).saveAsTextFile("hdfs:///output/movie_gender_avg")