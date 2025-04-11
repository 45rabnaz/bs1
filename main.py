import os
import sys
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

# Now import your spider
from bs.bs.spiders.bs1 import BusinessStandardSpider

def run_spider():
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    working_dir = 'tmp'
    cache = {}

    with open("output1.json", "a") as output_file:
        process.crawl(
            BusinessStandardSpider,
            output=output_file,
            working_dir=working_dir,
            cache=cache
        )
        process.start()

if __name__ == "__main__":
    run_spider()