import redis
import mechanicalsoup
import configparser 
from elasticsearch import Elasticsearch, helpers

config = configparser.ConfigParser()
config.read('elastic.ini')
print(config.read('elastic.ini'))

es = Elasticsearch(
    cloud_id= config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

def write_to_elastic(es, url, html):
    link = url.decode('utf-8')
    es.index(
        index= 'webpages', 
        document={
            'url': link,
            'html': html
        }
    )

def crawl(browser, r, es, url):
    print(url)
    browser.open(url)

    #cache to elastic 
    write_to_elastic(es, url, str(browser.page))

    #get links
    links = browser.page.find_all("a")
    hrefs = [a.get("href") for a in links]

    # Do filtering
    domain = "https://en.wikipedia.org"
    links = [domain + a for a in hrefs if a and a.startswith("/wiki/")]

    print("pushing links to redis")
    r.lpush("links", *links)

start_url = "https://en.wikipedia.org/wiki/Redis"
r = redis.Redis()
browser = mechanicalsoup.StatefulBrowser()

r.lpush("links", start_url)

while link := r.rpop("links"):
    crawl(browser, r, es, link)