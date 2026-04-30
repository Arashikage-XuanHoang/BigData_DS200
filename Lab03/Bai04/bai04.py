from pyspark import SparkContext
import csv
import os

sc = SparkContext()

# ==============================
# 1. Hàm phân nhóm tuổi
# ==============================
def age_group(age):
    age = int(age)
    if age < 18:
        return "Under18"
    elif age <= 25:
        return "18-25"
    elif age <= 35:
        return "26-35"
    elif age <= 45:
        return "36-45"
    elif age <= 50:
        return "46-50"
    else:
        return "50+"

# ==============================
# 2. UserID → AgeGroup
# ==============================
users = sc.textFile("hdfs:///input/users.txt") \
    .map(lambda x: x.split(",")) \
    .map(lambda x: (x[0], age_group(x[2])))
# (UserID, AgeGroup)

# ==============================
# 3. Movies (MovieID → Title)
# ==============================
movies = sc.textFile("hdfs:///input/movies.txt") \
    .map(lambda x: next(csv.reader([x]))) \
    .map(lambda x: (x[0], x[1]))
# (MovieID, Title)

# ==============================
# 4. Ratings
# ==============================
ratings = sc.textFile("hdfs:///input/ratings_1.txt") \
    .union(sc.textFile("hdfs:///input/ratings_2.txt")) \
    .map(lambda x: x.split(",")) \
    .map(lambda x: (x[0], (x[1], float(x[2]))))
# (UserID, (MovieID, Rating))

# ==============================
# 5. Join users + ratings
# ==============================
user_rating = ratings.join(users)
# (UserID, ((MovieID, Rating), AgeGroup))

# ==============================
# 6. (MovieID, AgeGroup) → Rating
# ==============================
movie_age_rating = user_rating.map(
    lambda x: ((x[1][0][0], x[1][1]), x[1][0][1])
)
# ((MovieID, AgeGroup), Rating)

# ==============================
# 7. Aggregate
# ==============================
agg = movie_age_rating.mapValues(lambda r: (r, 1)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

avg = agg.mapValues(lambda x: x[0] / x[1])
# ((MovieID, AgeGroup), Avg)

# ==============================
# 8. Join với movie để lấy title
# ==============================
result = avg.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
    .join(movies)
# (MovieID, ((AgeGroup, Avg), Title))

# ==============================
# 9. Format kết quả
# ==============================
final = result.map(
    lambda x: (x[1][1], x[1][0][0], x[1][0][1])
)
# (Title, AgeGroup, Avg)

# ==============================
# 10. IN KẾT QUẢ
# ==============================
print("\n=== Average Rating by Movie & Age Group ===")
for r in final.take(20):
    print(f"{r[0]} | {r[1]} | {r[2]:.2f}")

# ==============================
# 11. LƯU HDFS
# ==============================
output = final.map(lambda x: f"{x[0]},{x[1]},{x[2]:.2f}")

# xóa output cũ 
os.system("hdfs dfs -rm -r -f /output/movie_age_avg")

output.coalesce(1).saveAsTextFile("hdfs:///output/movie_age_avg")