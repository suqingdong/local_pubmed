from django.db import connection


main_sql = """
CREATE TABLE pubmed_articles_hash (
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
    -- 核心优化点 1：使用 halfvec 节省一半内存
    title_abstract_vec public.halfvec(1536),
    ts_en tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')), 'A') || 
        setweight(to_tsvector('english', coalesce(abstract, '')), 'B')
    ) STORED,
    -- 核心优化点 2：以 pmid 为主键
    PRIMARY KEY (pmid)
) PARTITION BY HASH (pmid);
"""



def create_partitions():
    with connection.cursor() as cursor:

        cursor.execute(main_sql)
        print("已创建主表: pubmed_articles_hash")

        for i in range(64):
            table_name = f"pubmed_part_hash_{i:02d}"
            sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} 
            PARTITION OF pubmed_articles_hash 
            FOR VALUES WITH (MODULUS 64, REMAINDER {i});
            """
            cursor.execute(sql)
            print(f"已创建子表: {table_name}")


create_partitions()
