#python3 apply.py -d 200000
#./elastic_index.py --local -i pagerank 
./search.py --local -i pagerank -m pr_full
./rate.py -t results/pr_top20 -r results/pr_rprecision -o results/pagerank_metrics
