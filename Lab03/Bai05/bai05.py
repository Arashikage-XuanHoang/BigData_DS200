from pyspark import SparkContext
import os

sc = SparkContext()

# ==============================
# 1. UserID → Occupation
# ==============================
users = sc.textFile("hdfs:///input/users.txt") \
    .map(lambda x: x.split(",")) \
    .map(lambda x: (x[0], x[3]))
# (UserID, OccupationID)

# ==============================
# 2. Map OccupationID → Name
# ==============================
occ = sc.textFile("hdfs:///input/occupation.txt") \
    .map(lambda x: x.split(",")) \
    .map(lambda x: (x[0], x[1]))
# (OccID, OccName)

# ==============================
# 3. Ratings
# ==============================
ratings = sc.textFile("hdfs:///input/ratings_1.txt") \
    .union(sc.textFile("hdfs:///input/ratings_2.txt")) \
    .map(lambda x: x.split(",")) \
    .map(lambda x: (x[0], float(x[2])))
# (UserID, Rating)

# ==============================
# 4. Join users + ratings
# ==============================
user_rating = ratings.join(users)
# (UserID, (Rating, OccupationID))

# ==============================
# 5. (OccupationID → (rating,1))
# ==============================
occ_rating = user_rating.map(
    lambda x: (x[1][1], (x[1][0], 1))
)

# ==============================
# 6. Aggregate
# ==============================
agg = occ_rating.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# ==============================
# 7. Tính average
# ==============================
avg = agg.mapValues(
    lambda x: (x[0] / x[1], x[1])  # (avg, count)
)

# ==============================
# 8. Join với occupation name
# ==============================
result = avg.join(occ)
# (OccID, ((avg, count), OccName))

final = result.map(
    lambda x: (x[1][1], x[1][0][0], x[1][0][1])
)
# (OccupationName, AvgRating, Count)

# ==============================
# 9. Sort theo rating giảm dần
# ==============================
final_sorted = final.sortBy(lambda x: -x[1])

# ==============================
# 10. IN KẾT QUẢ
# ==============================
print("\n=== Average Rating by Occupation ===")
for o in final_sorted.collect():
    print(f"{o[0]} | Avg: {o[1]:.2f} | Count: {o[2]}")

# ==============================
# 11. LƯU HDFS
# ==============================
output = final_sorted.map(
    lambda x: f"{x[0]},{x[1]:.2f},{x[2]}"
)

# xóa output cũ nếu có
os.system("hdfs dfs -rm -r -f /output/occupation_avg")

output.coalesce(1).saveAsTextFile("hdfs:///output/occupation_avg")