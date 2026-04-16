-- Bài 3: Xác định khía cạnh (aspect) có nhiều đánh giá tích cực nhất và tiêu cực nhất

-- Đọc dữ liệu đầu vào từ HDFS (đường dẫn đã upload ở bài trước)
reviews_raw = LOAD '/user/hadoop/lab02/input/hotel-review.csv'
              USING PigStorage(';')
              AS (id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

-- Lọc các dòng có sentiment xác định (chỉ giữ 'positive' và 'negative')
valid_reviews = FILTER reviews_raw BY (sentiment == 'positive' OR sentiment == 'negative');

-- Nhóm dữ liệu theo aspect và sentiment, sau đó đếm số lượng
grouped = GROUP valid_reviews BY (aspect, sentiment);
counts = FOREACH grouped GENERATE 
             group.aspect AS aspect,
             group.sentiment AS sentiment,
             COUNT(valid_reviews) AS cnt;

-- Tách riêng hai loại sentiment
positive_counts = FILTER counts BY sentiment == 'positive';
negative_counts = FILTER counts BY sentiment == 'negative';

-- ===== Tìm aspect có nhiều đánh giá tích cực nhất =====
-- Sắp xếp giảm dần theo cnt và lấy bản ghi đầu tiên
ordered_positive = ORDER positive_counts BY cnt DESC;
top_positive = LIMIT ordered_positive 1;

-- ===== Tìm aspect có nhiều đánh giá tiêu cực nhất =====
ordered_negative = ORDER negative_counts BY cnt DESC;
top_negative = LIMIT ordered_negative 1;

-- Lưu kết quả vào HDFS
STORE top_positive INTO '/user/hadoop/lab02/output_positive_aspect' USING PigStorage('\t');
STORE top_negative INTO '/user/hadoop/lab02/output_negative_aspect' USING PigStorage('\t');