-- bai05_clean.pig
-- Tính TF-IDF và lấy top 5 từ theo (aspect, category)

-- Load stopwords
stopwords = LOAD 'input/stopwords.txt' USING PigStorage() AS (stopword:chararray);

-- Load data
raw = LOAD 'input/hotel-review.csv' USING PigStorage(';') AS (id:int, comment:chararray, aspect:chararray, category:chararray, sentiment:chararray);

-- Filter empty comments
raw = FILTER raw BY comment IS NOT NULL AND comment != '';

-- Lowercase and tokenize
lowercased = FOREACH raw GENERATE id, LOWER(comment) AS comment_lower, aspect, category, sentiment;
tokenized = FOREACH lowercased GENERATE id, FLATTEN(TOKENIZE(comment_lower)) AS word, aspect, category, sentiment;

-- Remove stopwords
joined = JOIN tokenized BY word LEFT OUTER, stopwords BY stopword;
filtered = FILTER joined BY stopwords::stopword IS NULL;
clean = FOREACH filtered GENERATE tokenized::id AS id, tokenized::word AS word, tokenized::aspect AS aspect, tokenized::category AS category;
clean = FILTER clean BY word IS NOT NULL AND word != '';

-- Term frequency per (aspect, category, word)
term_group = GROUP clean BY (aspect, category, word);
term_freq = FOREACH term_group GENERATE FLATTEN(group) AS (aspect, category, word), COUNT(clean) AS tf;

-- Total terms per (aspect, category)
group_total = GROUP clean BY (aspect, category);
total_terms = FOREACH group_total GENERATE FLATTEN(group) AS (aspect, category), COUNT(clean) AS total_terms;

-- Join to compute TF
tf_joined = JOIN term_freq BY (aspect, category), total_terms BY (aspect, category);
tf = FOREACH tf_joined GENERATE 
    term_freq::aspect AS aspect, 
    term_freq::category AS category, 
    term_freq::word AS word, 
    (double)term_freq::tf / (double)total_terms::total_terms AS tf_val;

-- Document frequency: number of distinct (aspect, category) pairs containing the word
-- We need to group clean by word, then for each word count distinct (aspect, category)
-- First create a distinct pair per word
word_ac_pairs = FOREACH clean GENERATE word, (aspect, category) AS ac_pair;
distinct_pairs = DISTINCT word_ac_pairs;
doc_freq = FOREACH (GROUP distinct_pairs BY word) GENERATE group AS word, COUNT(distinct_pairs) AS df;

-- Total number of distinct (aspect, category) groups
all_ac_pairs = FOREACH (GROUP clean BY (aspect, category)) GENERATE group AS ac_pair;
total_groups = FOREACH (GROUP all_ac_pairs ALL) GENERATE COUNT(all_ac_pairs) AS total_g;

-- Compute TF-IDF
tf_with_df = JOIN tf BY word LEFT OUTER, doc_freq BY word;
tf_idf_all = CROSS tf_with_df, total_groups;
tf_idf = FOREACH tf_idf_all GENERATE 
    tf::aspect AS aspect, 
    tf::category AS category, 
    tf::word AS word, 
    tf::tf_val AS tf_val,
    tf::tf_val * LOG((double)total_groups::total_g / (double)doc_freq::df) AS tf_idf_val;

-- For each (aspect, category), get top 5 by tf_idf_val
grouped = GROUP tf_idf BY (aspect, category);
top5 = FOREACH grouped {
    sorted = ORDER tf_idf BY tf_idf_val DESC;
    limited = LIMIT sorted 5;
    GENERATE FLATTEN(limited);
};

-- Store output
STORE top5 INTO 'Bai05/output_bai05' USING PigStorage();