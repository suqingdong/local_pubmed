# 创建二级分区
python manage.py run_sql sql/partition_level2.sql

# 导入数据
python manage.py run_sql sql/insert_level2.sql

# 创建索引
nohup python script/index.py &> index_level2.log &

# ANALYZE 分区
nohup parallel -j 4 < analyze.sh &> analyze_level2.log &
nohup python manage.py run_sql "ANALYZE pubmed_articles_2;" &> analyze_level2_main.log &
