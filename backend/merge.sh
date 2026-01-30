cat ../data/2025/baseline/*jl ../data/2025/updatefiles/*jl > ../data/all.jl

python ../tests/merge.py
