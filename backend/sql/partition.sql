-- 1. 创建主分区表 (注意最后是分号)
DROP TABLE IF EXISTS pubmed_articles_new CASCADE;

CREATE TABLE pubmed_articles_new (
    pmid INTEGER NOT NULL,
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
    year INTEGER NOT NULL,
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
    title_abstract_vec vector(1536),
    ts_en tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')), 'A') || 
        setweight(to_tsvector('english', coalesce(abstract, '')), 'B')
    ) STORED,
    PRIMARY KEY (pmid, year)
) PARTITION BY RANGE (year);

-- 2. 独立创建子表 (每个都是独立的语句)

-- 历史区间
CREATE TABLE pubmed_part_history 
    PARTITION OF pubmed_articles_new FOR VALUES FROM (MINVALUE) TO (2001);

-- 2001-2010
CREATE TABLE pubmed_part_2001_2010 
    PARTITION OF pubmed_articles_new FOR VALUES FROM (2001) TO (2011);

-- 2011-2020
CREATE TABLE pubmed_part_2011_2020 
    PARTITION OF pubmed_articles_new FOR VALUES FROM (2011) TO (2021);

-- 2021-2025 (包含 2021, 2022, 2023, 2024, 2025)
CREATE TABLE pubmed_part_2021_2025 
    PARTITION OF pubmed_articles_new FOR VALUES FROM (2021) TO (2026);

-- 未来区间 (2026+)
CREATE TABLE pubmed_part_future 
    PARTITION OF pubmed_articles_new FOR VALUES FROM (2026) TO (MAXVALUE);