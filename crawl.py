import sqlite3
import mechanicalsoup
import configparser
from neo4j import GraphDatabase
import redis

config = configparser.ConfigParser()
config.read('database.ini')  # Update with your database configuration

class SQLiteDriver:
    def __init__(self, db_path):
        self._connection = sqlite3.connect(db_path)
        self._cursor = self._connection.cursor()
        self._create_table()

    def _create_table(self):
        self._cursor.execute('''
            CREATE TABLE IF NOT EXISTS webpages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT,
                html TEXT
            )
        ''')
        self._connection.commit()

    def write_to_db(self, url, html):
        self._cursor.execute('INSERT INTO webpages (url, html) VALUES (?, ?)', (url, html))
        self._connection.commit()

    def close(self):
        self._connection.close()

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

def crawl(browser, r, sqlite_driver, neo4j_driver, url):
    print(url)
    browser.open(url)

    # Cache to SQLite
    write_to_db(url, str(browser.page))

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

def write_to_db(url, html):
    sqlite_driver.write_to_db(url, html)

# Neo4j connection details
neo4j_uri = "bolt://localhost:7689"
neo4j_user = "neo4j"
neo4j_password = "5ppu3nf9aml"

# SQLite database connection details
sqlite_db_path = "webpages.db"  # Update with your desired SQLite database path
sqlite_driver = SQLiteDriver(sqlite_db_path)

# Start URL and initialization
start_url = "https://en.wikipedia.org/wiki/Redis"
r = redis.Redis()
browser = mechanicalsoup.StatefulBrowser()
neo4j_driver = Neo4jDriver(neo4j_uri, neo4j_user, neo4j_password)

r.lpush("links", start_url)

while link := r.rpop("links"):
    crawl(browser, r, sqlite_driver, neo4j_driver, link)

# Close SQLite and Neo4j drivers
sqlite_driver.close()
neo4j_driver.close()
