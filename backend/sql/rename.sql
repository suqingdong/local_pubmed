BEGIN;

-- 1. 备份旧表（以防万一），上线新表
ALTER TABLE IF EXISTS pubmed_articles RENAME TO pubmed_articles_old;
ALTER TABLE pubmed_articles_new RENAME TO pubmed_articles;

-- 2. 在父表上“定义”索引（这一步只是在父表注册元数据，不扫数据，极快）
-- 这样你查询 pubmed_articles 时，优化器才知道去子表找对应的索引
CREATE INDEX idx_pubmed_vec_global ON pubmed_articles USING hnsw (title_abstract_vec vector_cosine_ops);
CREATE INDEX idx_pubmed_ts_global ON pubmed_articles USING gin (ts_en);
CREATE INDEX idx_pubmed_year_global ON pubmed_articles (year);
CREATE INDEX idx_pubmed_factor_global ON pubmed_articles (factor);

COMMIT;
