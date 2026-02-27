-- 开启独立会话执行
SET maintenance_work_mem = '32GB';

CREATE INDEX IF NOT EXISTS idx_vec_2021_2025 ON pubmed_part_2021_2025 
USING hnsw (title_abstract_vec vector_cosine_ops) 
WITH (m = 16, ef_construction = 64);

-- 加速英文标题和摘要的全文检索
CREATE INDEX IF NOT EXISTS idx_gin_ts_en_2021_2025 ON pubmed_part_2021_2025 USING GIN (ts_en);

-- 加速 PMID 查找
CREATE INDEX IF NOT EXISTS idx_pmid_2021_2025 ON pubmed_part_2021_2025 (pmid);

-- 加速影响因子/分值过滤
CREATE INDEX IF NOT EXISTS idx_factor_2021_2025 ON pubmed_part_2021_2025 (factor DESC NULLS LAST);
