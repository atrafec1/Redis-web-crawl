import redis
import mechanicalsoup
import configparser
from elasticsearch import Elasticsearch
from neo4j import GraphDatabase

config = configparser.ConfigParser()
config.read('elastic.ini')

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    basic_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)

class Neo4jDriver:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def create_node(self, tx, url):
        tx.run("CREATE (:WebPage {url: $url})", url=url)

    def create_relationship(self, tx, from_url, to_url):
        tx.run(
            "MATCH (from:WebPage {url: $from_url}) "
            "MATCH (to:WebPage {url: $to_url}) "
            "CREATE (from)-[:LINKS_TO]->(to)",
            from_url=from_url,
            to_url=to_url
        )

def write_to_elastic(es, url, html):
    link = url.decode('utf-8')
    es.index(
        index='webpages',
        document={
            'url': link,
            'html': html
        }
    )

def crawl(browser, r, es, neo4j_driver, url):
    print(url)
    browser.open(url)

    # Cache to elastic
    write_to_elastic(es, url, str(browser.page))

    # Create node in Neo4j
    with neo4j_driver._driver.session() as session:
        session.write_transaction(neo4j_driver.create_node, url)

    # Get links
    links = browser.page.find_all("a")
    hrefs = [a.get("href") for a in links]

    # Do filtering
    domain = "https://en.wikipedia.org"
    links = [domain + a for a in hrefs if a and a.startswith("/wiki/")]

    print("pushing links to redis and creating relationships in Neo4j")
    for link in links:
        r.lpush("links", link)
        with neo4j_driver._driver.session() as session:
            session.write_transaction(neo4j_driver.create_relationship, url, link)

# Neo4j connection details
neo4j_uri = "bolt://localhost:7689"
neo4j_user = "neo4j"
neo4j_password = "5ppu3nf9aml"

# Start URL and initialization
start_url = "https://en.wikipedia.org/wiki/Redis"
r = redis.Redis()
browser = mechanicalsoup.StatefulBrowser()
neo4j_driver = Neo4jDriver(neo4j_uri, neo4j_user, neo4j_password)

r.lpush("links", start_url)

while link := r.rpop("links"):
    crawl(browser, r, es, neo4j_driver, link)

# Close Neo4j driver
neo4j_driver.close()
