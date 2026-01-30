DROP TABLE IF EXISTS pubmed_articles_new CASCADE;

-- 创建主分区表（手动定义结构，去掉 title_abstract_vector）
CREATE TABLE pubmed_articles_new (
    pmid INTEGER PRIMARY KEY,
    title VARCHAR(2000),
    abstract TEXT,
    journal VARCHAR(500),
    med_abbr VARCHAR(500),
    iso_abbr VARCHAR(500),
    pubdate VARCHAR(500),
    pubmed_pubdate DATE,
    pmc VARCHAR(500),
    issn VARCHAR(500),
    e_issn VARCHAR(500),
    doi VARCHAR(500),
    year INTEGER,
    pagination VARCHAR(500),
    volume VARCHAR(500),
    issue VARCHAR(500),
    pub_status VARCHAR(500),
    authors JSONB,
    keywords JSONB,
    pub_types JSONB,
    author_mail JSONB,
    author_first VARCHAR(500),
    author_last VARCHAR(500),
    affiliations JSONB,
    abstract_cn TEXT,
    factor FLOAT,
    jcr VARCHAR(10),
    zky VARCHAR(10),
    title_abstract_vec vector(1536), -- 只保留 1536 维
    ts_en tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')), 'A') || 
        setweight(to_tsvector('english', coalesce(abstract, '')), 'B')
    ) STORED
) PARTITION BY HASH (pmid);

-- 创建 4 个子表
CREATE TABLE pubmed_part_0 PARTITION OF pubmed_articles_new FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE pubmed_part_1 PARTITION OF pubmed_articles_new FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE pubmed_part_2 PARTITION OF pubmed_articles_new FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE pubmed_part_3 PARTITION OF pubmed_articles_new FOR VALUES WITH (MODULUS 4, REMAINDER 3);


-- 4. 暴力搬运数据（此时无索引，纯磁盘写入，最快）
-- 建议在会话中增大 memory 提高搬运效率
SET maintenance_work_mem = '64GB';
-- 明确列出字段进行插入（排除 ts_en）
INSERT INTO pubmed_articles_new (
    pmid, title, abstract, journal, med_abbr, iso_abbr, 
    pubdate, pubmed_pubdate, pmc, issn, e_issn, doi, 
    year, pagination, volume, issue, pub_status, 
    authors, keywords, pub_types, author_mail, 
    author_first, author_last, affiliations, 
    abstract_cn, factor, jcr, zky, 
    title_abstract_vec
) 
SELECT 
    pmid, title, abstract, journal, med_abbr, iso_abbr, 
    pubdate, pubmed_pubdate, pmc, issn, e_issn, doi, 
    year, pagination, volume, issue, pub_status, 
    authors, keywords, pub_types, author_mail, 
    author_first, author_last, affiliations, 
    abstract_cn, factor, jcr, zky, 
    title_abstract_vec
FROM pubmed_articles;

-- 搬运完后，立即分析子表，确保统计信息准确
ANALYZE pubmed_part_0;
ANALYZE pubmed_part_1;
ANALYZE pubmed_part_2;
ANALYZE pubmed_part_3;

-- 检查下最终行数是否对齐（确认没丢数据）
SELECT count(*) FROM pubmed_articles_new;