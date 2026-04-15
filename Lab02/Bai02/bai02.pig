-- =========================
-- LOAD DATA
-- =========================

reviews = LOAD '/mnt/0498342198341420/UIT_DS_Third_Year_Second_Semester_2025_2026/BigData_DS200/Lab02/input/hotel-review.csv'
USING PigStorage(';')
AS (id:int, review:chararray, category:chararray, subcategory:chararray, sentiment:chararray);

stopwords = LOAD '/mnt/0498342198341420/UIT_DS_Third_Year_Second_Semester_2025_2026/BigData_DS200/Lab02/input/stopwords.txt'
USING PigStorage()
AS (word:chararray);

-- =========================
-- BƯỚC 1: TIỀN XỬ LÝ TEXT
-- =========================

-- lowercase + remove ký tự đặc biệt
reviews_clean = FOREACH reviews GENERATE 
    id,
    REPLACE(LOWER(review), '[^\\p{L}\\s]', '') AS review,
    category,
    subcategory,
    sentiment;

-- tokenize
tokens = FOREACH reviews_clean GENERATE 
    id,
    TOKENIZE(review) AS words;

words_flat = FOREACH tokens GENERATE 
    id,
    FLATTEN(words) AS word;

-- remove stopwords bằng JOIN
joined = JOIN words_flat BY word LEFT OUTER, stopwords BY word;

filtered = FILTER joined BY stopwords::word IS NULL;

result_words = FOREACH filtered GENERATE 
    words_flat::id AS id, 
    words_flat::word AS word;

-- loại từ rỗng
result_words_clean = FILTER result_words BY word IS NOT NULL AND word != '';

-- =========================
-- BÀI 2.1: WORD COUNT (>500)
-- =========================

group_words = GROUP result_words_clean BY word;

word_count = FOREACH group_words GENERATE 
    group AS word,
    COUNT(result_words_clean) AS freq;

word_over_500 = FILTER word_count BY freq > 500;

word_sorted = ORDER word_over_500 BY freq DESC;

-- =========================
-- BÀI 2.2: ĐẾM CATEGORY
-- =========================

-- tránh trùng (vì 1 review có nhiều dòng)
distinct_reviews = DISTINCT reviews;

group_category = GROUP distinct_reviews BY category;

category_count = FOREACH group_category GENERATE 
    group AS category,
    COUNT(distinct_reviews) AS total;

category_sorted = ORDER category_count BY total DESC;

-- =========================
-- BÀI 2.3: ĐẾM ASPECT (subcategory)
-- =========================

group_aspect = GROUP reviews BY subcategory;

aspect_count = FOREACH group_aspect GENERATE 
    group AS aspect,
    COUNT(reviews) AS total;

aspect_sorted = ORDER aspect_count BY total DESC;

-- =========================
-- OUTPUT
-- =========================

STORE word_sorted INTO 'output_wordcount' USING PigStorage(',');
STORE category_sorted INTO 'output_category' USING PigStorage(',');
STORE aspect_sorted INTO 'output_aspect' USING PigStorage(',');

-- debug (nếu cần)
-- DUMP word_sorted;
-- DUMP category_sorted;
-- DUMP aspect_sorted;