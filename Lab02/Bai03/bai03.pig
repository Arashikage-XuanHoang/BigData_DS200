-- =========================
-- LOAD DATA
-- =========================

reviews = LOAD '/mnt/0498342198341420/UIT_DS_Third_Year_Second_Semester_2025_2026/BigData_DS200/Lab02/input/hotel-review.csv'
USING PigStorage(';')
AS (id:int, review:chararray, category:chararray, subcategory:chararray, sentiment:chararray);

-- =========================
-- TÁCH POSITIVE / NEGATIVE
-- =========================

positive_reviews = FILTER reviews BY sentiment == 'positive';
negative_reviews = FILTER reviews BY sentiment == 'negative';

-- =========================
-- ĐẾM THEO ASPECT (subcategory)
-- =========================

-- POSITIVE
group_pos = GROUP positive_reviews BY subcategory;

pos_count = FOREACH group_pos GENERATE 
    group AS aspect,
    COUNT(positive_reviews) AS total_positive;

-- NEGATIVE
group_neg = GROUP negative_reviews BY subcategory;

neg_count = FOREACH group_neg GENERATE 
    group AS aspect,
    COUNT(negative_reviews) AS total_negative;

-- =========================
-- SẮP XẾP GIẢM DẦN
-- =========================

pos_sorted = ORDER pos_count BY total_positive DESC;
neg_sorted = ORDER neg_count BY total_negative DESC;

-- =========================
-- LẤY TOP 1
-- =========================

top_positive = LIMIT pos_sorted 1;
top_negative = LIMIT neg_sorted 1;

-- =========================
-- OUTPUT
-- =========================

STORE pos_sorted INTO 'output_positive_aspect' USING PigStorage(',');
STORE neg_sorted INTO 'output_negative_aspect' USING PigStorage(',');

STORE top_positive INTO 'output_top_positive' USING PigStorage(',');
STORE top_negative INTO 'output_top_negative' USING PigStorage(',');

-- Debug
-- DUMP top_positive;
-- DUMP top_negative;