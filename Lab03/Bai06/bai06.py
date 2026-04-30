from pyspark import SparkContext
import datetime
import os

sc = SparkContext()

# ==============================
# 1. Hàm convert timestamp → year
# ==============================
def get_year(ts):
    return datetime.datetime.fromtimestamp(int(ts)).year

# ==============================
# 2. Đọc ratings
# ==============================
ratings = sc.textFile("hdfs:///input/ratings_1.txt") \
    .union(sc.textFile("hdfs:///input/ratings_2.txt")) \
    .map(lambda x: x.split(",")) \
    .map(lambda x: (get_year(x[3]), float(x[2])))
# (Year, Rating)

# ==============================
# 3. (Year → (rating,1))
# ==============================
year_rating = ratings.mapValues(lambda r: (r, 1))

# ==============================
# 4. Reduce
# ==============================
agg = year_rating.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# ==============================
# 5. Tính average
# ==============================
result = agg.mapValues(
    lambda x: (x[0] / x[1], x[1])  # (avg, count)
)

# ==============================
# 6. Sort theo năm
# ==============================
result_sorted = result.sortByKey()

# ==============================
# 7. IN KẾT QUẢ
# ==============================
print("\n=== Rating Analysis by Year ===")
for year, (avg, count) in result_sorted.collect():
    print(f"{year} | Avg: {avg:.2f} | Count: {count}")

# ==============================
# 8. LƯU HDFS
# ==============================
output = result_sorted.map(
    lambda x: f"{x[0]},{x[1][0]:.2f},{x[1][1]}"
)

# xóa output cũ nếu có
os.system("hdfs dfs -rm -r -f /output/year_analysis")

output.coalesce(1).saveAsTextFile("hdfs:///output/year_analysis")