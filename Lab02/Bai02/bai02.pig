-- Bài 2: Thống kê trên dữ liệu hotel-review
-- Input: /user/hadoop/lab02/input/hotel-review.csv (phân cách bằng ';')
-- Output: 
--   1. WordCount (từ xuất hiện >500 lần): /user/hadoop/lab02/output_bai02_wordcount
--   2. Category count: /user/hadoop/lab02/output_bai02_category
--   3. Aspect count: /user/hadoop/lab02/output_bai02_aspect

-- Đọc dữ liệu thô
reviews_raw = LOAD '/user/hadoop/lab02/input/hotel-review.csv'
              USING PigStorage(';')
              AS (id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

-- ========== 1. Thống kê tần số từ (có loại stopword để kết quả ý nghĩa) ==========
-- Đọc stopwords (giả sử vẫn dùng file stopwords.txt đã upload)
stopwords_raw = LOAD '/user/hadoop/lab02/input/stopwords.txt' AS (stopword:chararray);

-- Chuyển review về chữ thường và tách từ
reviews_lower = FOREACH reviews_raw GENERATE LOWER(review) AS text_lower;
tokenized = FOREACH reviews_lower {
    words = TOKENIZE(text_lower);
    GENERATE FLATTEN(words) AS word;
}

-- Loại bỏ stopword bằng LEFT JOIN
joined = JOIN tokenized BY word LEFT OUTER, stopwords_raw BY stopword;
filtered = FILTER joined BY stopwords_raw::stopword IS NULL;
clean_words = FOREACH filtered GENERATE tokenized::word AS word;

-- Đếm tần số từ
word_groups = GROUP clean_words BY word;
word_counts = FOREACH word_groups GENERATE group AS word, COUNT(clean_words) AS freq;

-- Lọc từ có tần số > 500
high_freq_words = FILTER word_counts BY freq > 500;

-- Sắp xếp giảm dần theo tần số (tùy chọn)
ordered_high_freq = ORDER high_freq_words BY freq DESC;

STORE ordered_high_freq INTO '/user/hadoop/lab02/output_bai02_wordcount' USING PigStorage('\t');

-- ========== 2. Thống kê số bình luận theo category ==========
category_groups = GROUP reviews_raw BY category;
category_counts = FOREACH category_groups GENERATE group AS category, COUNT(reviews_raw) AS count;
STORE category_counts INTO '/user/hadoop/lab02/output_bai02_category' USING PigStorage('\t');

-- ========== 3. Thống kê số bình luận theo aspect ==========
aspect_groups = GROUP reviews_raw BY aspect;
aspect_counts = FOREACH aspect_groups GENERATE group AS aspect, COUNT(reviews_raw) AS count;
STORE aspect_counts INTO '/user/hadoop/lab02/output_bai02_aspect' USING PigStorage('\t');