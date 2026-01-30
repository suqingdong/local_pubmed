-- -- 1. 删除主表上的全局索引（这会同步删除 part_3, 1, 2, 3 的旧索引）
-- DROP INDEX IF EXISTS idx_pubmed_vec_global;

-- -- 1. 删除旧的大索引
DROP INDEX IF EXISTS idx_hnsw_part_3;

-- 2. 建立瘦身版索引 (m 从 12 降到 8)
-- 维持 ef_construction 为 64 保证基本的构图质量
CREATE INDEX idx_hnsw_part_3 ON pubmed_part_3 
USING hnsw (title_abstract_vec vector_cosine_ops) 
WITH (m = 8, ef_construction = 64);

