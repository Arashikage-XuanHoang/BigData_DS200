-- bai01.pig
-- Tiền xử lý: lowercase, tokenize, loại bỏ stop words
-- Dữ liệu vào: input/hotel-review.csv (; delimiter, không header)
-- Stop words: input/stopwords.txt

-- Đọc stop words
stopwords = LOAD 'input/stopwords.txt' USING PigStorage() AS (stopword:chararray);

-- Đọc dữ liệu review
raw = LOAD 'input/hotel-review.csv' 
       USING PigStorage(';') 
       AS (id:int, comment:chararray, aspect:chararray, category:chararray, sentiment:chararray);

-- Loại bỏ dòng comment rỗng
raw = FILTER raw BY comment IS NOT NULL AND comment != '';

-- Lowercase comment
lowercased = FOREACH raw GENERATE 
              id, 
              LOWER(comment) AS comment_lower,
              aspect, category, sentiment;

-- Tokenize (tách từ theo khoảng trắng)
tokenized = FOREACH lowercased GENERATE 
            id,
            FLATTEN(TOKENIZE(comment_lower)) AS word,
            aspect, category, sentiment;

-- Loại bỏ stop word (left join với stopwords, giữ lại word không khớp)
joined = JOIN tokenized BY word LEFT OUTER, stopwords BY stopword;
filtered = FILTER joined BY stopwords::stopword IS NULL;

-- Gom nhóm theo id để tạo lại danh sách từ đã lọc
grouped = GROUP filtered BY id;

-- Lấy các từ đã lọc và giữ lại các trường aspect, category, sentiment (lấy từ dòng đầu mỗi nhóm)
final = FOREACH grouped {
          first = LIMIT filtered 1;
          words = FOREACH filtered GENERATE word;
          GENERATE group AS id, words AS filtered_words, FLATTEN(first.aspect) AS aspect, FLATTEN(first.category) AS category, FLATTEN(first.sentiment) AS sentiment;
        };

-- Lưu kết quả (có thể lưu dưới dạng CSV hoặc text)
STORE final INTO 'Bai01/output_bai01' USING PigStorage();