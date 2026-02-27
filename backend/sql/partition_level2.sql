
CREATE TABLE pubmed_articles_2 (
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

DO $$
BEGIN
    -- 1. History (< 2001): 8个子表
    CREATE TABLE p_history PARTITION OF pubmed_articles_2 FOR VALUES FROM (MINVALUE) TO (2001) PARTITION BY HASH (pmid);
    FOR i IN 0..7 LOOP
        EXECUTE format('CREATE TABLE p_history_h%s PARTITION OF p_history FOR VALUES WITH (MODULUS 8, REMAINDER %s)', i, i);
    END LOOP;

    -- 2. 2001-2010: 8个子表
    CREATE TABLE p_2001_2010 PARTITION OF pubmed_articles_2 FOR VALUES FROM (2001) TO (2011) PARTITION BY HASH (pmid);
    FOR i IN 0..7 LOOP
        EXECUTE format('CREATE TABLE p_2001_2010_h%s PARTITION OF p_2001_2010 FOR VALUES WITH (MODULUS 8, REMAINDER %s)', i, i);
    END LOOP;

    -- 3. 2011-2020: 16个子表 (数据密集)
    CREATE TABLE p_2011_2020 PARTITION OF pubmed_articles_2 FOR VALUES FROM (2011) TO (2021) PARTITION BY HASH (pmid);
    FOR i IN 0..15 LOOP
        EXECUTE format('CREATE TABLE p_2011_2020_h%s PARTITION OF p_2011_2020 FOR VALUES WITH (MODULUS 16, REMAINDER %s)', i, i);
    END LOOP;

    -- 4. 2021-2025: 16个子表 (核心区域)
    CREATE TABLE p_2021_2025 PARTITION OF pubmed_articles_2 FOR VALUES FROM (2021) TO (2026) PARTITION BY HASH (pmid);
    FOR i IN 0..15 LOOP
        EXECUTE format('CREATE TABLE p_2021_2025_h%s PARTITION OF p_2021_2025 FOR VALUES WITH (MODULUS 16, REMAINDER %s)', i, i);
    END LOOP;

    -- 5. Future (2026+): 4个子表
    CREATE TABLE p_future PARTITION OF pubmed_articles_2 FOR VALUES FROM (2026) TO (MAXVALUE) PARTITION BY HASH (pmid);
    FOR i IN 0..3 LOOP
        EXECUTE format('CREATE TABLE p_future_h%s PARTITION OF p_future FOR VALUES WITH (MODULUS 4, REMAINDER %s)', i, i);
    END LOOP;
END $$;
