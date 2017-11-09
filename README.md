# Web-Crawler
A simple python crawler with BFS and Pagerank based priority queue. The project contains following files:

py_crawler.py: Python script for crawler which could be run as 

>> python py_crawler.py

Beware that the program would require an API key from bing. You can find one at azure.com

It takes the following three inputs:

search_term: the query for the focused crawler
Method: bfs or pagerank
num_pages: Number of pages to be crawled

The python script was used to generate the following four logs:

1. ebbets field_bfs.log
2. knuckle sandwich_bfs.log
3. ebbets field_pagerank.log
4. knuckle sandwich_pagerank.log

Each of the log files contain 1000 crawled urls and other relevant information.
