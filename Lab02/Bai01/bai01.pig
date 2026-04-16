-- Bước 1: Đọc dữ liệu từ hotel-review.csv (phân cách bằng dấu ';')
reviews_raw = LOAD '/user/hadoop/lab02/input/hotel-review.csv'
              USING PigStorage(';') 
              AS (id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

-- Bước 2: Đọc danh sách stopwords (mỗi dòng một từ)
stopwords_raw = LOAD '/user/hadoop/lab02/input/stopwords.txt' AS (stopword:chararray);

-- Bước 3: Xử lý từng bình luận
--    a) Chuyển về chữ thường (LOWER)
--    b) Tách thành các từ riêng biệt (TOKENIZE)
--    c) Loại bỏ stopword
reviews_lower = FOREACH reviews_raw GENERATE 
                    id, 
                    LOWER(review) AS text_lower;

-- Tách từ và trải phẳng thành mỗi dòng một từ
tokenized = FOREACH reviews_lower {
    words = TOKENIZE(text_lower);
    GENERATE id, FLATTEN(words) AS word;
}

-- Kết hợp với stopwords để loại bỏ (dùng LEFT JOIN)
joined = JOIN tokenized BY word LEFT OUTER, stopwords_raw BY stopword;

-- Giữ lại những từ không nằm trong danh sách stopword
filtered = FILTER joined BY stopwords_raw::stopword IS NULL;

-- Chọn các trường cần thiết để lưu trữ
clean_words = FOREACH filtered GENERATE 
                  tokenized::id AS review_id, 
                  tokenized::word AS clean_word;

-- Bước 4: Lưu kết quả (mỗi từ trên một dòng)
STORE clean_words INTO '/user/hadoop/lab02/output_bai01' USING PigStorage('\t');