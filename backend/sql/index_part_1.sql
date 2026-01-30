-----------------------------------------------------------
-- 窗口 1：pubmed_part_1 全量索引构建脚本
-----------------------------------------------------------

-- 1. 性能环境配置
SET maintenance_work_mem = '40GB'; -- 4窗口并行时建议40G，单窗口可给64G
SET hnsw.ef_search = 64;           -- 提高构建时的搜索质量

-- 2. HNSW 向量索引 (最耗时，排在第一位)
-- 预计 500 万行耗时：4-6 小时
CREATE INDEX idx_hnsw_part_1 ON pubmed_part_1 
USING hnsw (title_abstract_vec vector_cosine_ops) 
WITH (m = 12, ef_construction = 64);

-- 3. 全文检索索引 (GIN 索引)
-- 针对生成列 ts_en，构建速度取决于文本长度
CREATE INDEX idx_ts_en_part_1 ON pubmed_part_1 USING gin (ts_en);

-- 4. 常用字段 B-tree 索引 (构建极快，通常几分钟)
-- pmid 索引
CREATE INDEX idx_pmid_part_1 ON pubmed_part_1 (pmid);
-- year 索引
CREATE INDEX idx_year_part_1 ON pubmed_part_1 (year);
-- factor 索引 (影响因子)
CREATE INDEX idx_factor_part_1 ON pubmed_part_1 (factor);

-- 5. 验证当前分区的索引是否全部创建成功
SELECT indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) 
FROM pg_stat_user_indexes 
WHERE relname = 'pubmed_part_1';