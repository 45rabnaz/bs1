import re
import json
import scrapy
from datetime import datetime
from urllib.parse import urljoin
from fake_useragent import UserAgent
from .factory import Factory

class BusinessStandardSpider(Factory):
    name = "business_standard"
    allowed_domains = ["business-standard.com"]
    start_urls = ["https://www.business-standard.com/"]
    hostname = "business-standard.com"
    
    article_pattern = re.compile(r"^https://www\.business-standard\.com/.*-(\d{12})_1\.html$")
    blacklist_patterns = [
        r"/webstories", r"/video-gallery", r"/cricket", r"/entertainment",
        r"/health", r"/companies/result", r"/education", r"/budget",
        r"/sports", r"/lifestyle", r"/book", r"/photos", r"/podcasts",
        r"/opinion", r"/technology", r"/management", r"/hindi\.business-standard\.com",
        r"/hindi", r"/crossword-puzzle-online", r"/sudoku-puzzle-online", r"/emi-calculator-tool", 
        r"/tax-calculator-tool", r"/gold-rate-today", r"/silver-rate-today", r"/about-us", r"/nse-top-losers-indices-undefined"
        r"/quarterly-results", r"/ipo", r"/ipo/recent-ipos-list", r"/stock-companies-list", r"/author"
        r"/newsletters", r"/moil-ltd-share-price-13863.html", r"/portfolio", r"/indusind-bank-ltd-share-price-5531/announcements" 
        r"/announcements"
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ua = UserAgent(fallback="Mozilla/5.0")
        self.crawled_urls = set()

    def get_headers(self, referer=None):
        """Generate headers with proper referer chain"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': referer if referer else f'https://{self.hostname}/'
        }

    def start_requests(self):
        # Initial request with self-referer
        yield scrapy.Request(
            url=self.start_urls[0],
            headers=self.get_headers(),
            callback=self.parse,
            errback=self.handle_error
        )

    def handle_error(self, failure):
        self.logger.warning(f"Request failed: {failure.request.url}")
        yield failure.request.copy().replace(dont_filter=True)

    def parse(self, response):
        if 'hindi.business-standard.com' in response.url.lower():
         self.logger.debug(f"Skipping Hindi URL: {response.url}")
         return
        
        if response.status != 200:
            self.logger.error(f"Failed to access {response.url}")
            return

        # Skip non-HTML responses (like PDFs)
        if not response.headers.get('Content-Type', b'').startswith(b'text/html'):
            return

        for link in response.css("a::attr(href)").getall():
            full_url = urljoin(response.url, link)
            
            if not self.is_valid_url(full_url) or full_url in self.crawled_urls:
                continue
            
            self.crawled_urls.add(full_url)

            if self.article_pattern.match(full_url):
                yield scrapy.Request(
                    url=full_url,
                    headers=self.get_headers(referer=response.url),
                    callback=self.save_news_article,
                    meta={'article_url': full_url},
                    dont_filter=True
                )
            else:
                yield scrapy.Request(
                    url=full_url,
                    headers=self.get_headers(referer=response.url),
                    callback=self.parse,
                    dont_filter=True
                )

    def get_article_id(self, response_or_url):
        """Safe extraction that handles both Response objects and URL strings"""
        url = response_or_url.url if hasattr(response_or_url, 'url') else str(response_or_url)
        match = self.article_pattern.match(url)
        return match.group(1) if match else None

    def get_article_title(self, response):
        article_data = self._extract_ld_json(response)
        return article_data.get("headline", "").strip() or response.css("h1::text").get(default="").strip()

    def get_article_body(self, article_id, response):
        article_data = self._extract_ld_json(response)
        if article_data and "articleBody" in article_data:
            return article_data["articleBody"].strip()
        return " ".join(response.css("div.p-content p::text").getall()).strip()

    def get_updated_at(self, response):
        article_data = self._extract_ld_json(response)
        if article_data:
            date_str = article_data.get("dateModified", article_data.get("datePublished", ""))
            if date_str:
                try:
                    return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z").strftime(self.DATE_OUTPUT_FORMAT)
                except ValueError:
                    pass
        return datetime.now().strftime(self.DATE_OUTPUT_FORMAT)

    def get_keywords(self, response):
        article_data = self._extract_ld_json(response)
        if article_data:
            keywords = article_data.get("keywords", [])
            return [kw.strip() for kw in keywords.split(",")] if isinstance(keywords, str) else keywords
        return []

    def _extract_ld_json(self, response):
        """Robust JSON-LD extraction with error handling"""
        for ld_json in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(ld_json)
                if isinstance(data, dict) and data.get("@type") == "NewsArticle":
                    return data
            except json.JSONDecodeError:
                continue
        return None