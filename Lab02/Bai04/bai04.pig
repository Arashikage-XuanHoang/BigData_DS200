-- Bài 4: Top 5 từ tích cực và tiêu cực cho từng category
-- Input: /user/hadoop/lab02/input/hotel-review.csv
-- Output: /user/hadoop/lab02/output_top_positive_words (cho positive)
--         /user/hadoop/lab02/output_top_negative_words (cho negative)

-- Đọc dữ liệu review
reviews_raw = LOAD '/user/hadoop/lab02/input/hotel-review.csv'
              USING PigStorage(';')
              AS (id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

-- Đọc stopwords
stopwords_raw = LOAD '/user/hadoop/lab02/input/stopwords.txt' AS (stopword:chararray);

-- Lọc chỉ lấy positive và negative, bỏ qua sentiment khác nếu có
valid_reviews = FILTER reviews_raw BY (sentiment == 'positive' OR sentiment == 'negative');

-- Xử lý văn bản: lowercase, tokenize, loại bỏ stopword
-- Bước 1: lowercase
reviews_lower = FOREACH valid_reviews GENERATE 
                    id,
                    category,
                    sentiment,
                    LOWER(review) AS text_lower;

-- Bước 2: tokenize thành từng từ
tokenized = FOREACH reviews_lower {
    words = TOKENIZE(text_lower);
    GENERATE id, category, sentiment, FLATTEN(words) AS word;
}

-- Bước 3: loại bỏ stopword bằng LEFT JOIN
joined = JOIN tokenized BY word LEFT OUTER, stopwords_raw BY stopword;
filtered = FILTER joined BY stopwords_raw::stopword IS NULL;

-- Chọn các trường cần thiết
clean_data = FOREACH filtered GENERATE 
                 tokenized::category AS category,
                 tokenized::sentiment AS sentiment,
                 tokenized::word AS word;

-- Đếm tần số từ theo (category, sentiment, word)
word_groups = GROUP clean_data BY (category, sentiment, word);
word_counts = FOREACH word_groups GENERATE 
                  group.category AS category,
                  group.sentiment AS sentiment,
                  group.word AS word,
                  COUNT(clean_data) AS freq;

-- Gom nhóm theo (category, sentiment) để lấy top 5
grouped_by_cat_sent = GROUP word_counts BY (category, sentiment);

-- Sử dụng nested FOREACH để sắp xếp và lấy top 5 cho mỗi nhóm
top_5_per_group = FOREACH grouped_by_cat_sent {
    -- Sắp xếp các bản ghi trong bag theo freq giảm dần
    sorted = ORDER word_counts BY freq DESC;
    -- Lấy 5 bản ghi đầu tiên
    top5 = LIMIT sorted 5;
    GENERATE 
        group.category AS category,
        group.sentiment AS sentiment,
        top5.(word, freq) AS top_words;  -- bag gồm các tuple (word, freq)
}

-- Tách riêng positive và negative để lưu
positive_top5 = FILTER top_5_per_group BY sentiment == 'positive';
negative_top5 = FILTER top_5_per_group BY sentiment == 'negative';

-- Lưu kết quả
STORE positive_top5 INTO '/user/hadoop/lab02/output_top_positive_words' USING PigStorage('\t');
STORE negative_top5 INTO '/user/hadoop/lab02/output_top_negative_words' USING PigStorage('\t');