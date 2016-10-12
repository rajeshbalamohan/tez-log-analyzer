In some cases only yarn-app logs are available for the tez application and it might be difficult to
go through each line to understand the issue.

This module tries to extract relevant information of interest and provides it as a report.txt.

Note that this is just for creating a report out of the logs and identify the what can be the
issue. It does not provide any suggestions/prescriptions for modifying any settings.

Examples include the following:
1. Version Details
2. Digraph details of the DAG so that visualized using "dot -Tpdf digraph.dot > digraph.pdf"
3. Split Details (in case of multiple DAGs, analyze by time stamp. E.g Dag1 and Dag10 can have Map 1)
4. Stuck Task Analyzer (replace task by attempt & search in Task Attempt Started to known node details)
5. Vertex Mapping
6. Vertex Finished
7. Shuffle related issues (e.g when some shuffle error happens, it might spin into "shuffle
blamed for" exception in infinite loops in certain versions. This helps in identifying whether
source was the problem or the destination was the problem)
8. Task Attempt Started
9. Task Attempt Finished
10. Rack Resolver.. (machine to rack level mappings, number of nodes participated in the job)
11. machine --> attempt --> container mapping
12. hashTable load timings (in cae of hive job)


