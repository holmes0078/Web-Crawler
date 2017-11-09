#Python Web Crawler

#Author: Tushar Ahuja
from __future__ import division
import urllib2
from urlparse import urlparse, urljoin
import simplejson
from py_ms_cognitive import PyMsCognitiveWebSearch
import config
from Queue import PriorityQueue, Queue
from bs4 import BeautifulSoup, SoupStrainer
import urlnorm
import robotparser
from timeout import timeout
import re
import socket
import os, errno
import distutils.dir_util
import httplib
import logging
import sys
import tldextract
import time


# web crawler class with all functions defined inside the class's scope
class WebCrawler:
    def __init__(self, method, keyword, total_pages, logger):
        self.graph = {} # Graph for storing link relationships
        self.visitedUrls = {} # Dict to store already visited URL's
        self.ranks = {} # Storing Pagerank for each link
        self.domain_dict = {} # A domain dict to limit url's per domain
        self.log_dict = {} # A log dictionary for file logging
        self.method = method # Method: Pagerank or BFS
        self.nodesToVisit = PriorityQueue() # Pagerank Queue
        self.bfs_queue = Queue() # BFS queue
        self.totalNodes = int(total_pages) # Total urls to be crawled
        self.nodeCount = 0 # Counter for total urls crawled
        self.total_size_crawled = 0 # Counter for total size of urls crawled
        self.max_per_domain = 10 # Max no of url's per domain
        self.interval = 30 # Interval after which pagerank should run
        self.keyword = keyword # The search term
        self.urlIndex = 1 # File Name Index: Files stores as 1.html, 2.html, etc.
        self.FileNotFoundError = 0 # Counter for number of 4xx errors
        self.logger = logger # Python Logger
        self.get_seed(self.keyword) # Call PyBing API to get first 10 search results
        self.reciprocal_depth = 3

        if self.method == 'pagerank':
            self.pagerank_crawl()
            self.final_ranks = self.compute_ranks(self.graph, self.reciprocal_depth)
        elif self.method == 'bfs':
            self.bfs_crawl()

    # Get first 10 results from Bing Search Engine and add them to priority queue with priority -0.1
    # which is also it's pagerank (0.1) as initially a pagerank of 1 is divided amongst 10 pages
    def get_seed(self, search_term):
        search_service = PyMsCognitiveWebSearch(config.key, search_term)
        first_twenty_result = search_service.search(limit=10, format='json') #1-10

        for i in range(10):
            #Giving Each of the 10 links a pagerank of 0.1 (i.e. 1/10)
            link = first_twenty_result[i].json['displayUrl']
            link = self.normalize(link)
            self.nodesToVisit.put((-0.1, link)) #Negate for priority queue to favor higher pagerank
            self.bfs_queue.put(link)
            self.ranks[link] = 0.1

    # A BFS crawl
    def bfs_crawl(self):
        while self.totalNodes >= self.nodeCount:
            url = self.bfs_queue.get()
            visited = url in self.visitedUrls

            #Parameter for imposing max limit of 10 per domain
            try:
                ext = tldextract.extract(url)
                domain_url = ext.domain
            except:
                domain_url = ''

            if domain_url in self.domain_dict:
                self.domain_dict[domain_url] += 1
            else:
                self.domain_dict[domain_url] = 1

            per_domain = self.domain_dict[domain_url]

            if not visited and per_domain <= self.max_per_domain:
                if self.can_fetch_url(url):
                    print self.nodeCount, "Fetching Url: ", url

                    self.log_dict['url'] = url.strip()

                    # Save File as html in ./data directory
                    resp_code, size = self.save_file(url, str(self.urlIndex))
                    self.urlIndex += 1 # Increment File Number

                    self.log_dict['http_resp'] = resp_code
                    self.log_dict['size'] = size

                    # Log info
                    self.logger.info("", extra=self.log_dict)

                    self.visitedUrls[url] = 1
                    self.nodeCount += 1 # increase node count

                    # Get links
                    links = self.getLinks(url)

                    if links:
                        links = [x for x in links if x is not None]
                        links = [self.normalize(x) for x in links] # normalize links
                        # validate links
                        links = [x for x in links if self.validateLink(x) is True]
                        links = list(set(links))

                        if links:
                            if url not in self.graph:
                                self.graph[url] = links # add links to graph

                            for link in links:
                                self.bfs_queue.put(link) # add links to bfs_queue

    # A pagerank based crawl
    def pagerank_crawl(self):
        if True:
            while self.nodeCount <= self.totalNodes and not self.nodesToVisit.empty():
                node = self.nodesToVisit.get()
                url = node[1]

                visited = url in self.visitedUrls # check if url already visited

                #Parameter for imposing max limit of 10 per domain
                try:
                    ext = tldextract.extract(url)
                    domain_url = ext.domain
                except:
                    domain_url = ''

                if domain_url in self.domain_dict:
                    self.domain_dict[domain_url] += 1
                else:
                    self.domain_dict[domain_url] = 1

                per_domain = self.domain_dict[domain_url]

                if not visited and per_domain <= self.max_per_domain:
                    if self.can_fetch_url(url):

                        self.log_dict['url'] = url.strip()

                        # get HTTP response code and save file
                        resp_code, size = self.save_file(url, str(self.urlIndex))
                        self.urlIndex += 1 # Increment File name Number

                        self.log_dict['http_resp'] = resp_code
                        self.log_dict['size'] = size

                        self.visitedUrls[url] = 1
                        self.nodeCount += 1 # increment crawled url counter

                        links = self.getLinks(url)

                        if links:
                            links = [x for x in links if x is not None]
                            links = [self.normalize(x) for x in links] # normalize link
                            # validate links
                            links = [x for x in links if self.validateLink(x) is True]
                            links = list(set(links)) # remove duplicated

                            if links:
                                if url not in self.graph:
                                    self.graph[url] = links
                                elif url in self.graph:
                                    for link in links:
                                        self.graph[url].append(link)

                                for link in links:
                                    if link not in self.graph:
                                        self.graph[link] = []

                                for link in links:
                                    # put links in priority queue with dummy page rank of 0.05
                                    self.nodesToVisit.put((-0.05, link))
                                    if link not in self.ranks:
                                        self.ranks[link] = 0.05

                        # Run Pagerank after the graph size increases by 30
                        if self.nodeCount >= self.interval:
                            new_ranks = self.compute_ranks(self.graph, 3) # Compute new ranks
                            self.reassign_priority(new_ranks) # Rebuild Priority Queue
                            self.interval += 30 # Increment interval
                            self.ranks

                        # Fill log dict with local pagerank
                        if url in self.ranks:
                            self.log_dict['local_pr'] = float(self.ranks[url])
                        else:
                            self.log_dict['local_pr'] = 0.0

                        self.logger.info("", extra=self.log_dict)

    def can_fetch_url(self, url):
        p = urlparse(url)
        robots_url = p.scheme + "://" + p.netloc + "/robots.txt"

        rp = robotparser.RobotFileParser()
        rp.set_url(robots_url)

        socket.setdefaulttimeout(8)

        try:
            rp.read()
        except socket.timeout:
            return False
        except:
            return False
        try:
            return rp.can_fetch("*", url)
        except:
            return False

    # function to save file contents and
    # return http header and file size
    def save_file(self, url, file_name):
        cwd = os.getcwd()
        path = cwd + '/data'
        distutils.dir_util.mkpath(path)

        size = 0.0
        status = None

        try:
            normalized_url = self.normalize(url)
            response = urllib2.urlopen(normalized_url, timeout = 2)
            fullfilename = os.path.join(path, file_name + '.html')

            #open the file for writing
            fh = open(fullfilename, "w")

            # read from request while writing to file

            fh.write(response.read())
            fh.close()

            size = os.stat(fullfilename).st_size
            size = size/1024

            self.total_size_crawled += size

        except urllib2.HTTPError as e:
            status = e.code
        except:
            pass

        if status is not None:
            status = str(status)
        else:
            status = str(200)

        try:
            if str(status) in ['400', '401', '403', '404', '410']:
                self.FileNotFoundError += 1
        except:
            pass

        return status, float(size)


    def normalize(self, url):
        parsed = urlparse(url.encode('utf-8'))

        if '//' not in url:
            url = '%s%s' % ('http://', url)

        if parsed.scheme == "http" or "https":
            try:
                normalized_url = urlnorm.norm(url)
            except:
                return None
            return normalized_url
        else:
            return None



    def getLinks(self, url):
        try:
            f = urllib2.urlopen(url, timeout = 2)
        except:
            return False

        link_list = []

        try:
            soup = BeautifulSoup(f, "lxml", parse_only = SoupStrainer(['a', 'description']))

            for link in soup.findAll('a', limit = 20, href = True):
                absolute_link = urljoin(url, link.get('href')) # Absolute Link
                link_list.append(absolute_link)
        except:
            pass

        if len(link_list) > 0:
            return link_list
        else:
            return False


    def validateLink(self, url):
        # Using stop words which is not scalable but improves program performance
        # for this homework
        stop_words = ['download', "javascript", 'cgi', 'file', 'upload']


        extensionBlackList = [".asx", ".avi", ".bmp", ".css", ".doc", ".docx",
                              ".flv", ".gif", ".jpeg", ".jpg", ".mid", ".mov",
                              ".mp3", ".ogg", ".pdf", ".png", ".ppt", ".ra",
                              ".ram", ".rm", ".swf", ".txt ", ".wav", ".wma",
                              ".wmv", ".xml", ".zip", ".m4a", ".m4v", ".mov",
                              ".mp4", ".m4b", ".cgi", ".svg", '.ogv',".dmg",
                              '.tar', '.gz']
        if url:
            for extension in extensionBlackList:
                    if extension in url.lower():
                        return False

            for stop_word in stop_words:
                    if stop_word in url.lower():
                        return False
        else:
            return False

        return True

    # compute page rank using the random surfer model
    def compute_ranks(self, graph, k):
        print "Computing Page Rank"
        d = 0.8
        numloops = 10
        self.ranks = {}
        npages = len(graph)
        for page in graph:
            self.ranks[page] = 1.0 / npages
        for i in range(0, numloops):
            newranks = {}
            for page in graph:
                newrank = (1 - d) / npages
                for node in graph:
                    if page in graph[node]:
                        if not self._is_reciprocal_link(graph, node, page, k):
                            newrank = newrank + d*self.ranks[node]/len(graph[node])
                newranks[page] = newrank
            self.ranks = newranks
        return self.ranks

    def _is_reciprocal_link(self, graph, source, destination, k):
        if k==0:
            return source==destination
        if source in graph[destination]:
            return True
        for node in graph[destination]:
            if self._is_reciprocal_link(graph,source,node,k-1):
                return True
        return False

    def reassign_priority(self, new_ranks):
        new_pq = PriorityQueue()
        while not self.nodesToVisit.empty():
            node = self.nodesToVisit.get()
            url = node[1]
            old_priority = node[0]
            if url in new_ranks:
                priority = new_ranks[url]
                new_pq.put((-priority, url))
            else:
                new_pq.put((old_priority, url))

        self.nodesToVisit = new_pq

def main():

    search_term = raw_input('Enter Search Term --> ')
    method = raw_input('Enter Method Name: bfs or pagerank -->')
    total_pages = raw_input('Enter total pages to be crawled --> ')

    #Initialize Time
    start_time = time.time()

    if method == 'bfs':
        # BFS logger
        logger = logging.getLogger('PyCrawlerBfs')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)-15s  %(http_resp)-3s  %(url)s  %(size)-3.2f kb')
        fh = logging.FileHandler(search_term + '_' + 'bfs' + '.log')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # call web crawlers's init
        wb = WebCrawler(method, search_term, total_pages, logger)

        with open(search_term + '_' + method + '.log', 'a') as fh:
            fh.write("User Query                     :: %s \n" % search_term )
            fh.write("Number of Pages Crawled        :: %s \n" % str(wb.nodeCount - 1) )
            fh.write("Number of 4xx Erros            :: %i \n" % wb.FileNotFoundError )
            fh.write("Total size of downloaded Pages :: %.2f KB\n" % (wb.total_size_crawled) )
            fh.write("Total time                     :: %s seconds" % (time.time() - start_time) )

    elif method == 'pagerank':
        # PageRank Logger
        logger = logging.getLogger('PyCrawler')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)-15s  %(http_resp)-3s  %(url)s %(size)-3.2f kb  %(local_pr)-1.6f')
        filename = search_term + '_' + 'pagerank' + '.log'
        fh = logging.FileHandler(filename)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # call web crawlers's init
        wb = WebCrawler(method, search_term, total_pages, logger)

        # Read and update final page ranks
        file_lines = []
        with open(filename, 'r') as fh:
            for line in fh.readlines():
                parsed_list = line.split()
                url = parsed_list[3]
                old_rank = parsed_list[-1]
                if url in wb.final_ranks:
                    new_rank = wb.final_ranks[url]
                else:
                    new_rank = old_rank

                new_rank = "{0:.8f}".format(round(new_rank,1))

                parameter = '  '.join([line.strip(), str(new_rank), '\n'])

                file_lines.append(parameter)

        with open(filename, 'w') as fh:
            fh.writelines(file_lines)

        with open(search_term + '_' + method + '.log', 'a') as fh:
            fh.write("User Query                     :: %s \n" % search_term )
            fh.write("Number of Pages Crawled        :: %s \n" % str(wb.nodeCount - 1) )
            fh.write("Number of 4xx Erros            :: %i \n" % wb.FileNotFoundError )
            fh.write("Total size of downloaded Pages :: %.2f KB\n" % (wb.total_size_crawled) )
            fh.write("Total time                     :: %s seconds" % (time.time() - start_time) )

if __name__ == "__main__":
    main()
