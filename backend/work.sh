# 1. convert xml to jl
sh convert.sh

# 2. merge all jl files, remove duplicates
sh merge.sh

# 3和4可以同时进行
# 3. generate embeddings
split -l 2000000 -d --additional-suffix=.jl ../data/all_no_dup.jl ../data/split/all_no_dup_

ls ../data/split/all_no_dup_*.jl | xargs -i echo python ../tests/embedding_single.py -i {} -o {}_embeddings.jl.gz > embedding_commands.sh

nohup parallel -j 10 < embedding_commands.sh &> embedding_all.log &

# 4. import to database
# import ../data/all_no_dup.jl
nohup python manage.py init_pubmed_jl ../data/all_no_dup.jl &> init_pubmed_jl.log &
# import ../data/split/*_embeddings.jl.gz

# 先删除索引，再更新embedding
python manage.py index_pubmed -o remove title_abstract_vec_hnsw_idx

ls ../data/split/all_no_dup_*embeddings.jl.gz | xargs -i echo python manage.py embedding_update -i {} > embedding_update_commands.sh
nohup parallel -j 5 -u < embedding_update_commands.sh &> embedding_update.log &


# # 5. analyze and rebuild index
# nohup python manage.py analyze_pubmed -o analyze --full &> analyze.log &
# # 查看进度
# # python manage.py run_sql 'SELECT phase, heap_tuples_scanned, heap_tuples_written, index_rebuild_count FROM pg_stat_progress_cluster;'

# nohup python manage.py analyze_pubmed -o index &> index.log &
# # 查看进度
# python manage.py run_sql 'SELECT * FROM pg_stat_progress_create_index;'


# 使用分区表
nohup python manage.py run_sql sql/partition.sql &> partition.log &

# 子分区表建立索引
ls sql |grep index | xargs -i echo "nohup python manage.py run_sql sql/{} &> {}.log &"  > create_partition_index_commands.sh
sh create_partition_index_commands.sh

# 重命名主表
python manage.py run_sql sql/rename.sql

# 优化
python manage.py run_sql sql/analyze.sql

# 预热
python manage.py db_warmup

# 子表ANALYZE
nohup python manage.py run_sql "VACUUM VERBOSE ANALYZE pubmed_part_0;" &> analyze_part0.log &
nohup python manage.py run_sql "VACUUM VERBOSE ANALYZE pubmed_part_1;" &> analyze_part1.log &
nohup python manage.py run_sql "VACUUM VERBOSE ANALYZE pubmed_part_2;" &> analyze_part2.log &
nohup python manage.py run_sql "VACUUM VERBOSE ANALYZE pubmed_part_3;" &> analyze_part3.log &

# 全表ANALYZE
python manage.py run_sql "ANALYZE pubmed_articles;"


# ======
# 重建瘦身版索引
python manage.py run_sql "DROP INDEX IF EXISTS idx_pubmed_vec_global;"

python manage.py run_sql sql/reindex_part_0.sql
python manage.py run_sql sql/reindex_part_1.sql
python manage.py run_sql sql/reindex_part_2.sql
python manage.py run_sql sql/reindex_part_3.sql
