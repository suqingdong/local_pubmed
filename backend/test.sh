curl -s '172.17.244.207:8000/pubmed_api/search/' \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"q": "ngs", "year": 2024, "factor": 5, "top_k": 5, "start": 0}' 



# curl -s '172.17.244.207:8000/pubmed_api/search/?q=ngs&year=2024&factor=5&top_k=5&start=0'
curl -s '172.17.244.207:8000/pubmed_api/hybrid_search/?api_key=eb19aa6465c169ed57196cee6c3879cb&q=ngs'