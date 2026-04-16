-- Bài 4: Top 5 từ xuất hiện nhiều nhất trong mỗi category (bất kể sentiment)
-- Input: /user/hadoop/lab02/input/hotel-review.csv
-- Output: /user/hadoop/lab02/output_top_words_by_category

-- Đọc dữ liệu
reviews_raw = LOAD '/user/hadoop/lab02/input/hotel-review.csv'
              USING PigStorage(';')
              AS (id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

-- Đọc stopwords
stopwords_raw = LOAD '/user/hadoop/lab02/input/stopwords.txt' AS (stopword:chararray);

-- Tiền xử lý: lowercase, tokenize, loại bỏ stopword
reviews_lower = FOREACH reviews_raw GENERATE 
                    category,
                    LOWER(review) AS text_lower;

tokenized = FOREACH reviews_lower {
    words = TOKENIZE(text_lower);
    GENERATE category, FLATTEN(words) AS word;
}

joined = JOIN tokenized BY word LEFT OUTER, stopwords_raw BY stopword;
filtered = FILTER joined BY stopwords_raw::stopword IS NULL;

clean_data = FOREACH filtered GENERATE 
                 tokenized::category AS category,
                 tokenized::word AS word;

-- Đếm tần số từ theo category và word
word_groups = GROUP clean_data BY (category, word);
word_counts = FOREACH word_groups GENERATE 
                  group.category AS category,
                  group.word AS word,
                  COUNT(clean_data) AS freq;

-- Nhóm theo category để lấy top 5 từ mỗi category
grouped_by_category = GROUP word_counts BY category;

top5_per_category = FOREACH grouped_by_category {
    sorted = ORDER word_counts BY freq DESC;
    top5 = LIMIT sorted 5;
    GENERATE 
        group AS category,
        top5.(word, freq) AS top_words;
}

-- Lưu kết quả
STORE top5_per_category INTO '/user/hadoop/lab02/output_top_words_by_category' USING PigStorage('\t');