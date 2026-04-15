-- =====================================================
-- bai04.pig
-- Tìm top 5 từ positive và top 5 từ negative
-- theo từng cặp (aspect, category)
-- =====================================================

-- 1. Đọc stop words
stopwords = LOAD 'input/stopwords.txt' USING PigStorage() AS (stopword:chararray);

-- 2. Đọc dữ liệu review (giả sử không có header)
raw = LOAD 'input/hotel-review.csv' 
       USING PigStorage(';') 
       AS (id:int, comment:chararray, aspect:chararray, category:chararray, sentiment:chararray);

-- 3. Lọc bỏ comment rỗng
raw = FILTER raw BY comment IS NOT NULL AND comment != '';

-- 4. Chuyển chữ thường và tách từ (tokenize)
lowercased = FOREACH raw GENERATE 
             id, 
             LOWER(comment) AS comment_lower,
             aspect, category, sentiment;

tokenized = FOREACH lowercased GENERATE 
            id,
            FLATTEN(TOKENIZE(comment_lower)) AS word,
            aspect, category, sentiment;

-- 5. Loại bỏ stop word (left join và lọc null)
joined = JOIN tokenized BY word LEFT OUTER, stopwords BY stopword;
filtered = FILTER joined BY stopwords::stopword IS NULL;

-- 6. Chỉ giữ lại các cột cần thiết và loại bỏ từ rỗng
clean = FOREACH filtered GENERATE 
        tokenized::id AS id,
        tokenized::word AS word,
        tokenized::aspect AS aspect,
        tokenized::category AS category,
        tokenized::sentiment AS sentiment;

clean = FILTER clean BY word IS NOT NULL AND word != '';

-- 7. Đếm tần suất từ theo từng nhóm (aspect, category, sentiment, word)
word_counts = GROUP clean BY (aspect, category, sentiment, word);
counts = FOREACH word_counts GENERATE 
         FLATTEN(group) AS (aspect, category, sentiment, word),
         COUNT(clean) AS freq;

-- 8. Với mỗi nhóm (aspect, category, sentiment), sắp xếp theo freq giảm dần và lấy top 5
grouped_by_sentiment = GROUP counts BY (aspect, category, sentiment);

top_words = FOREACH grouped_by_sentiment {
    sorted = ORDER counts BY freq DESC;
    top = LIMIT sorted 5;
    GENERATE FLATTEN(top);
};

-- 9. Lưu kết quả (mỗi dòng: aspect, category, sentiment, word, freq)
STORE top_words INTO 'Bai04/output_top_words' USING PigStorage();

