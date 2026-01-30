ls ../data/2025/*/*gz | xargs -i echo python utils/load_pubmed.py  --n-years 0 {} > convert_jobs.sh

nohup parallel -j 8 < convert_jobs.sh &> convert_all.log &
