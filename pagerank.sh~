ALGORITHM=Pagerank
WORKERS=5     #包括workers和master的总数
GRAPH=input/pagerank/hashing
RESULT=result/pagerank/hashing
NODES=1000000 #statetable 
SNAPSHOT=0.1
TERMTHRESH=0.1
BUFMSG=100
PORTION=1



./maiter  --runner=$ALGORITHM --workers=$WORKERS --graph_dir=$GRAPH --result_dir=$RESULT --num_nodes=$NODES --snapshot_interval=$SNAPSHOT --portion=$PORTION --termcheck_threshold=$TERMTHRESH --bufmsg=$BUFMSG --v=0 > log


