# Web-Crawler
A simple python crawler with BFS and Pagerank based priority queue. The project contains following files:

py_crawler.py: Python script for crawler which could be run as 

**>> python py_crawler.py**

Beware that the program would require an API key from bing. You can find one at azure.com

It takes the following three inputs:

**search_term**: the query for the focused crawler

**Method**: bfs or pagerank

**num_pages**: Number of pages to be crawled

The python script was used to generate the following four logs:

1. ebbets field_bfs.log
2. knuckle sandwich_bfs.log
3. ebbets field_pagerank.log
4. knuckle sandwich_pagerank.log

Each of the log files contain 1000 crawled urls and other relevant information.

**Description:**
The python crawler has two settings

**1. BFS:** Uses a simple queue and crawls pages according to the BFS algorithm

**2. Page Rank:** Maintains a priority queue running page rank on graph each time after crawling 30 urls

**Major Functions:**


|   Function Name	   |                Description					       |      Library Used        |
| ------------------ | ----------------------------------------- | ------------------------ |
| get_seed	         |   Gets first 10 links from Bing			     |   PyBing                 |
| can_fetch_url	     |   Checks robots.txt for access allowance	 |	 Python RobotExclusion  | 
| save_file	         |   Saves html contents of crawled urls		 |   Python urllib          | 
| Save_file	         |   Catches various HTTP Error Codes		     |   Python urllib.HTTPError|
| Normalize	         |   Normalizes url and adds scheme (‘http’) |	 Python urlnorm         | 
| get_links	         |   Parsed the html file for links			     |   BeautifulSoup          | 
| validate_links	   |   Makes sure only html files are crawled	 |	 None                   |
| max_per_domain	   |   Rate Control					                   |   tldextract             | 

**Non Working Features**:

Haven’t catered to cased where cis.poly.edu is same as csserv2.poly.edu

