import redis
import mechanicalsoup as ms

r = redis.Redis()
browser = ms.StatefulBrowser()
start_url = "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", start_url)
def crawl(browser, r, url):
#Download url
    browser.open(url)
# Parse for more urls
    a_tags = browser.page.find_all("a")
    hrefs = [ a.get("href") for a in a_tags ]
    wikipedia_domain = "https://en.wikipedia.org"
    links = [ wikipedia_domain + a for a in hrefs if a and a.startswith("/wiki/")]
# Put urls in Redis queue
#create a linked list in Redis, call it “links”
    r.lpush("links", *links)
    link = r.rpop("links")
    if "Jesus" in str(link):
        print(link)
        print("FOUND JESUS")
        return
    else:
        print("CRAWL")
        crawl(browser, r, link)

    


crawl(browser, r, start_url)

#In terminal
# ps aux | grep redis
# brew services list
# redis-cli
# rpop links

