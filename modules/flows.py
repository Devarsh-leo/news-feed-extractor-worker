import json
import logging
import os
import time
from uuid import uuid4
from functools import partial
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timedelta

from modules.url_parser import UrlParser, XMLParser
from modules.keywords_manager import KeywordsManager
from modules.output_manager import OutputManager
from modules.helpers import paginate_filter_and_save_data,just_save_data
from modules.general import calculate_time_taken, validate_configs


max_threads = max(1, os.cpu_count() // 2)
thread_executor = ThreadPoolExecutor(max_workers=max_threads)

CONFIGS_FILE_PATH = "configs.json"
with open(CONFIGS_FILE_PATH, "r") as fp:
    configs = json.load(fp)
validate_configs(configs)


# client_url_mapping = configs["search_url_client_url_mapping"]
keywords_manager = KeywordsManager(configs)


def search_ft_markets(output_manager, url, url_id, from_date, to_date, kill_thread):
    # date format YYYY-MM-DD
    market_url = "https://www.ft.com/markets?page=1"
    time.sleep(0.01)
    try:
        market_url_parser = UrlParser(market_url)
        self = market_url_parser
        parent_next_page_selector = None
        next_page_selector = None

        logging.debug("Creating paginator")
        paginator = market_url_parser.get_paginator(
            get_next_page_selector=(parent_next_page_selector, next_page_selector),
            get_max_page_selector=None,
            max_page_regex=None,
        )
        # set(paginator)
        # get_url_by = {"attrs": {"aria-label": "Markets"}}
        # market_url = search_page_url_parser.find_all(
        #     by=get_url_by, many=False, get="href"
        # )
    except Exception as e:
        logging.error(f"Error while generating paginator: {e}")

    try:
        logging.debug("configuring selectors for feed extraction")
        match_keywords = partial(keywords_manager.match_keywords, url)
        site_url = url
        parent_selector = ".o-teaser--article"
        date_selector = ".o-teaser__timestamp-date"

        title_selector = ".o-teaser__heading a"

        title_body_parent_selector = None
        title_body_selector = "article p"
        # title_body_selector = "article#article-body p"
        # _ = market_url_parser.get_from_selector(
        #     title_body_parent_selector, title_body_selector, get="text"
        # )

        link_selector = ".o-teaser__heading a"

        author_parent_selector = None
        author_selector = ".article-info p"
        paginate_filter_and_save_data(
            output_manager,
            site_url,
            url_id,
            paginator,
            match_keywords,
            title_selector=(parent_selector, title_selector),
            link_selector=(parent_selector, link_selector),
            title_body_selector=(title_body_parent_selector, title_body_selector),
            date_selector=(parent_selector, date_selector),
            author_selector=(author_parent_selector, author_selector),
            from_date=from_date,
            to_date=to_date,
            visit_to_get=["author","body"],
            kill_thread=kill_thread,
        )

    except Exception as e:
        logging.error(f"Error while scrapping paginated data: {e}")
    # return scrapped_data
    return


def search_cityam_markets(output_manager, url, url_id, from_date, to_date, kill_thread):
    # date format YYYY-MM-DD

    # for keyword in keywords_manager.get_keywords(url):
    # time.sleep(0.01)
    # logging.info(f"current keyword: {keyword}")
    try:
        market_url = "https://www.cityam.com/category/markets/"
        market_url_parser = UrlParser(market_url)
        # self = market_url_parser
        parent_next_page_selector = None
        next_page_selector = ".next.page-numbers"

        parent_max_page_selector = "ul.page-numbers li"
        max_page_selector = "a"

        max_page_regex = "Page (.*)?"

        paginator = market_url_parser.get_paginator(
            get_next_page_selector=(parent_next_page_selector, next_page_selector),
            get_max_page_selector=(parent_max_page_selector, max_page_selector),
            max_page_regex=max_page_regex,
        )
    except Exception as e:
        logging.error(f"Error while generating paginator: {e}")

    try:
        logging.debug("configuring selectors for feed extraction")

        match_keywords = partial(keywords_manager.match_keywords, url)
        site_url = url

        self = market_url_parser
        date_parent_selector = "div.content-container article"
        date_selector = "time"

        title_parent_selector = "div.content-container article .card__title"
        title_selector = "a"

        title_body_parent_selector = None
        title_body_selector = "header~p"

        link_parent_selector = "div.content-container article .card__title"
        link_selector = "a"

        author_parent_selector = None
        author_selector = "[rel='author']"
        logging.debug("starting to scrape paginated urls")
        paginate_filter_and_save_data(
            output_manager,
            site_url,
            url_id,
            paginator,
            match_keywords,
            title_selector=(title_parent_selector, title_selector),
            link_selector=(link_parent_selector, link_selector),
            title_body_selector=(title_body_parent_selector, title_body_selector),
            date_selector=(date_parent_selector, date_selector),
            author_selector=(author_parent_selector, author_selector),
            from_date=from_date,
            to_date=to_date,
            visit_to_get=["body", "author"],
            kill_thread=kill_thread,
        )
        # set(
        #     map(
        #         scrapped_data.extend,
        #         (
        #             _data
        #             for _data in [
        #                 list(data) for data in scrapped_generator_data.values()
        #             ]
        #         ),
        #     )
        # )
    except Exception as e:
        logging.error(f"Error while scrapping paginated data: {e}")
    # return scrapped_data
    return


def search_reuters_fundnews(
    output_manager, site_url, url_id, from_date, to_date, kill_thread
):
    # date format YYYY-MM-DD

    # for keyword in keywords_manager.get_keywords(url):
    # time.sleep(0.01)
    # logging.info(f"current keyword: {keyword}")
    try:
        market_url = """https://www.reuters.com/pf/api/v3/content/fetch/articles-by-section-alias-or-id-v1?query={"arc-site":"reuters","called_from_a_component":true,"fetch_type":"collection","offset":21,"section_id":"/markets/funds/","size":9,"website":"reuters"}&d=179&_website=reuters"""
        market_url_parser = UrlParser(market_url)
        self = market_url_parser
        parent_next_page_selector = None
        next_page_selector = None
        # self.get_from_selector(
        #     parent_next_page_selector, next_page_selector, get="href"
        # )
        # parent_max_page_selector = None
        # max_page_selector = None

        # max_page_regex = "Page (.*)?"

        paginator = market_url_parser.get_paginator(
            get_next_page_selector=(parent_next_page_selector, next_page_selector),
            get_max_page_selector=None,
            max_page_regex=None,
        )
        # next(paginator)
    except Exception as e:
        logging.error(f"Error while generating paginator: {e}")

    try:
        logging.debug("configuring selectors for feed extraction")

        match_keywords = partial(keywords_manager.match_keywords, site_url)

        self = market_url_parser
        parent_selector = None
        date_parent_selector = parent_selector
        date_selector = "articles display_time"
        # _ = self.soup.select(date_parent_selector)
        # len(_)
        # self.get_from_selector(date_parent_selector, date_selector, get="text")
        # self.url

        title_parent_selector = parent_selector
        title_selector = "articles title"
        # _ = self.get_from_selector(title_parent_selector, title_selector, get="text")

        title_body_parent_selector = None
        title_body_selector = "p[data-testid*='paragraph'] ,p[class*='paragraph']"
        # _ = self.get_from_selector(
        #     title_body_parent_selector, title_body_selector, get="text"
        # )

        link_parent_selector = parent_selector
        link_selector = "articles canonical_url"
        # "https://www.reuters.com"
        # _ = self.get_from_selector(link_parent_selector, link_selector, get="href")

        author_parent_selector = "articles > ul > ul > authors"
        author_selector = "name"
        # _ = self.get_from_selector(author_parent_selector, author_selector, get="text")
        logging.debug("starting to scrape paginated urls")
        paginate_filter_and_save_data(
            output_manager,
            site_url,
            url_id,
            paginator,
            match_keywords,
            title_selector=(title_parent_selector, title_selector),
            link_selector=(link_parent_selector, link_selector),
            title_body_selector=(title_body_parent_selector, title_body_selector),
            date_selector=(date_parent_selector, date_selector),
            author_selector=(author_parent_selector, author_selector),
            from_date=from_date,
            to_date=to_date,
            visit_to_get=["body"],
            kill_thread=kill_thread,
        )
        # set(
        #     map(
        #         scrapped_data.extend,
        #         (
        #             _data
        #             for _data in [
        #                 list(data) for data in scrapped_generator_data.values()
        #             ]
        #         ),
        #     )
        # )
    except Exception as e:
        logging.error(f"Error while scrapping paginated data: {e}")
    # return scrapped_data
    return


def search_hargreaves_lansdown_funds(
    output_manager, url, url_id, from_date, to_date, kill_thread
):
    logging.debug("Starting search for search_hargreaves_lansdown_funds")
    # date format YYYY-MM-DD
    # First verify endpoint is working
    try:
        homepage = "https://www.hl.co.uk/news/tags/funds"
        homepage_parser = UrlParser(homepage, max_retries=5, timeout=15)
        assert homepage_parser.soup, f"Failed to load homepage : {homepage}"
        text = "\n".join(
            [
                i.text.replace(" ", "")
                for i in homepage_parser.soup.select("main#mainContent script")
            ]
        )
        assert (
            "articleEndPointListing='15077628'" in text
            and "document.location.href+'?SQ_DESIGN_NAME=blank&SQ_PAINT_LAYOUT_NAME=tagging_pagnation'"
            in text
        ), "HL page structure might be modified."
    except Exception as e:
        logging.error(f"Error while loading main page of: {e}")
    try:
        market_url = "https://www.hl.co.uk/news/tags/funds?SQ_DESIGN_NAME=blank&SQ_PAINT_LAYOUT_NAME=tagging_pagnation&result_15077628_result_page=1"
        market_url_parser = UrlParser(market_url, max_retries=3, timeout=15)
        assert market_url_parser.soup, f"Failed to load url: {url}"
        logging.debug("market_url loaded successfully")
        self = market_url_parser
        parent_next_page_selector = None
        next_page_selector = None
        # self.get_from_selector(
        #     parent_next_page_selector, next_page_selector, get="href"
        # )
        # parent_max_page_selector = None
        # max_page_selector = None

        # max_page_regex = "Page (.*)?"
        logging.debug("Creating paginator")
        paginator = market_url_parser.get_paginator(
            get_next_page_selector=(parent_next_page_selector, next_page_selector),
            get_max_page_selector=None,
            max_page_regex=None,
        )
        # next(paginator)
    except Exception as e:
        logging.error(f"Error while generating paginator: {e}")

    try:
        logging.debug("configuring selectors for feed extraction")

        match_keywords = partial(keywords_manager.match_keywords, url)
        site_url = url

        # self = market_url_parser
        parent_selector = ".newsCard__content"
        date_parent_selector = parent_selector
        date_selector = ".newsCard__date"
        # _ = self.soup.select(date_parent_selector)
        # len(_)
        # _ = self.get_from_selector(date_parent_selector, date_selector, get="text")
        # self.url

        title_parent_selector = parent_selector
        title_selector = ".newsCard__title"
        # _ = self.get_from_selector(title_parent_selector, title_selector, get="text")

        title_body_parent_selector = None
        title_body_selector = "#article div[class='row'] p,#article  div[class='row'] h2, #mainContent div[class='row'] p,#mainContent div[class='row'] h1"
        # _ = self.get_from_selector(
        #     title_body_parent_selector, title_body_selector, get="text"
        # )

        link_parent_selector = parent_selector
        link_selector = ".newsCard__anchor"
        # _ = self.get_from_selector(link_parent_selector, link_selector, get="href")

        author_parent_selector = parent_selector
        author_selector = ".newsCard__author"
        # _ = self.get_from_selector(author_parent_selector, author_selector, get="text")

        logging.debug("starting to scrape paginated urls")
        paginate_filter_and_save_data(
            output_manager,
            site_url,
            url_id,
            paginator,
            match_keywords,
            title_selector=(title_parent_selector, title_selector),
            link_selector=(link_parent_selector, link_selector),
            title_body_selector=(title_body_parent_selector, title_body_selector),
            date_selector=(date_parent_selector, date_selector),
            author_selector=(author_parent_selector, author_selector),
            from_date=from_date,
            to_date=to_date,
            visit_to_get=["body"],
            timeout=12,
            kill_thread=kill_thread,
        )
        # set(
        #     map(
        #         scrapped_data.extend,
        #         (
        #             _data
        #             for _data in [
        #                 list(data) for data in scrapped_generator_data.values()
        #             ]
        #         ),
        #     )
        # )
    except Exception as e:
        logging.error(f"Error while scrapping paginated data: {e}")
    # return scrapped_data
    return


def search_investmentweek_funds(
    output_manager, url, url_id, from_date, to_date, kill_thread
):
    logging.debug("Starting search for search_investmentweek_funds")
    # date format YYYY-MM-DD

    try:
        market_url = "https://www.investmentweek.co.uk/category/investment/funds"
        market_url_parser = UrlParser(market_url, timeout=5)
        assert market_url_parser.soup, f"Failed to load url: {url}"
        logging.debug("market_url loaded successfully")
        self = market_url_parser
        parent_next_page_selector = None
        next_page_selector = "a.next_page"
        # self.get_from_selector(
        #     parent_next_page_selector, next_page_selector, get="href"
        # )
        # parent_max_page_selector = None
        # max_page_selector = None

        # max_page_regex = "Page (.*)?"
        logging.debug("Creating paginator")
        paginator = market_url_parser.get_paginator(
            get_next_page_selector=(parent_next_page_selector, next_page_selector),
            get_max_page_selector=None,
            max_page_regex=None,
        )
        # next(paginator)
    except Exception as e:
        logging.error(f"Error while generating paginator: {e}")

    try:
        logging.debug("configuring selectors for feed extraction")

        match_keywords = partial(keywords_manager.match_keywords, url)
        site_url = url

        # self = market_url_parser
        parent_selector = ".card-body"
        date_parent_selector = parent_selector
        date_selector = ".published"
        # _ = self.soup.select(date_parent_selector)
        # len(_)
        _ = self.get_from_selector(date_parent_selector, date_selector, get="text")
        # self.url

        title_parent_selector = parent_selector
        title_selector = ".platformheading"
        # _ = self.get_from_selector(title_parent_selector, title_selector, get="text")

        title_body_parent_selector = None
        title_body_selector = ".summary, div[itemprop='articleBody']"
        _ = self.get_from_selector(
            title_body_parent_selector, title_body_selector, get="text"
        )

        link_parent_selector = parent_selector
        link_selector = ".platformheading a"
        # _ = self.get_from_selector(link_parent_selector, link_selector, get="href")

        author_parent_selector = None
        author_selector = ".article-head-block .author-name"
        # _ = self.get_from_selector(author_parent_selector, author_selector, get="text")

        logging.debug("starting to scrape paginated urls")
        paginate_filter_and_save_data(
            output_manager,
            site_url,
            url_id,
            paginator,
            match_keywords,
            title_selector=(title_parent_selector, title_selector),
            link_selector=(link_parent_selector, link_selector),
            title_body_selector=(title_body_parent_selector, title_body_selector),
            date_selector=(date_parent_selector, date_selector),
            author_selector=(author_parent_selector, author_selector),
            from_date=from_date,
            to_date=to_date,
            visit_to_get=["body", "author"],
            timeout=5,
            kill_thread=kill_thread,
        )
        # set(
        #     map(
        #         scrapped_data.extend,
        #         (
        #             _data
        #             for _data in [
        #                 list(data) for data in scrapped_generator_data.values()
        #             ]
        #         ),
        #     )
        # )
    except Exception as e:
        logging.error(f"Error while scrapping paginated data: {e}")
    # return scrapped_data
    return


def search_etfstream(output_manager, url, url_id, from_date, to_date, kill_thread):
    logging.debug("Starting search for search_etfstream")
    # date format YYYY-MM-DD

    try:
        market_url = "https://www.etfstream.com/news/page/1"
        logging.info(f"Processing url: {market_url}")
        market_url_parser = UrlParser(market_url, timeout=5)
        assert market_url_parser.soup, f"Failed to load url: {url}"
        logging.debug("market_url loaded successfully")
        self = market_url_parser
        parent_next_page_selector = None
        next_page_selector = None
        # self.get_from_selector(
        #     parent_next_page_selector, next_page_selector, get="href"
        # )
        # parent_max_page_selector = None
        # max_page_selector = None

        # max_page_regex = "Page (.*)?"
        logging.debug("Creating paginator")
        paginator = market_url_parser.get_paginator(
            get_next_page_selector=(parent_next_page_selector, next_page_selector),
            get_max_page_selector=None,
            max_page_regex=None,
        )
        # next(paginator)
    except Exception as e:
        logging.error(f"Error while generating paginator: {e}")

    try:
        logging.debug("configuring selectors for feed extraction")

        match_keywords = partial(keywords_manager.match_keywords, url)
        site_url = url

        # self = market_url_parser
        parent_selector = ".article-details"
        date_parent_selector = parent_selector
        date_selector = "time"
        # _ = self.soup.select(date_parent_selector)
        # len(_)
        # _ = self.get_from_selector(date_parent_selector, date_selector, get="text")
        # self.url

        title_parent_selector = parent_selector
        title_selector = "h4"
        # _ = self.get_from_selector(title_parent_selector, title_selector, get="text")

        title_body_parent_selector = None
        title_body_selector = "article"
        _ = self.get_from_selector(
            title_body_parent_selector, title_body_selector, get="text"
        )

        link_parent_selector = None
        link_selector = "a:has(.article-container)"
        # _ = self.get_from_selector(link_parent_selector, link_selector, get="href")

        author_parent_selector = parent_selector
        author_selector = ".article-author-date p"
        # _ = self.get_from_selector(author_parent_selector, author_selector, get="text")

        logging.debug("starting to scrape paginated urls")
        paginate_filter_and_save_data(
            output_manager,
            site_url,
            url_id,
            paginator,
            match_keywords,
            title_selector=(title_parent_selector, title_selector),
            link_selector=(link_parent_selector, link_selector),
            title_body_selector=(title_body_parent_selector, title_body_selector),
            date_selector=(date_parent_selector, date_selector),
            author_selector=(author_parent_selector, author_selector),
            from_date=from_date,
            to_date=to_date,
            visit_to_get=["body"],
            timeout=5,
            kill_thread=kill_thread,
        )
    # set(
    #     map(
    #         scrapped_data.extend,
    #         (
    #             _data
    #             for _data in [
    #                 list(data) for data in scrapped_generator_data.values()
    #             ]
    #         ),
    #     )
    # )
    except Exception as e:
        logging.error(f"Error while scrapping paginated data: {e}")
    # return scrapped_data
    return


def search_morningstar_fund_research(
    output_manager, url, url_id, from_date, to_date, kill_thread
):
    logging.debug("Starting search for search_morningstar_fund_research")
    # date format YYYY-MM-DD

    try:
        market_url = "https://www.morningstar.co.uk/uk/collection/2114/fund-research--insights.aspx?page=1"
        market_url_parser = UrlParser(market_url, timeout=5)
        assert market_url_parser.soup, f"Failed to load url: {url}"
        logging.debug("market_url loaded successfully")
        self = market_url_parser
        parent_next_page_selector = None
        next_page_selector = None
        # self.get_from_selector(
        #     parent_next_page_selector, next_page_selector, get="href"
        # )
        # parent_max_page_selector = None
        # max_page_selector = None

        # max_page_regex = "Page (.*)?"
        logging.debug("Creating paginator")
        paginator = market_url_parser.get_paginator(
            get_next_page_selector=(parent_next_page_selector, next_page_selector),
            get_max_page_selector=None,
            max_page_regex=None,
        )
        # next(paginator)
    except Exception as e:
        logging.error(f"Error while generating paginator: {e}")

    try:
        logging.debug("configuring selectors for feed extraction")

        match_keywords = partial(keywords_manager.match_keywords, url)
        site_url = url

        # self = market_url_parser
        parent_selector = None
        date_parent_selector = parent_selector
        date_selector = "td[headers='archive_date']"
        # _ = self.soup.select(date_parent_selector)
        # len(_)
        # _ = self.get_from_selector(date_parent_selector, date_selector, get="text")
        # self.url

        title_parent_selector = parent_selector
        title_selector = "td[headers='archive_title']"
        # _ = self.get_from_selector(title_parent_selector, title_selector, get="text")

        title_body_parent_selector = None
        title_body_selector = ".seopurpose h2, .seopurpose p"
        # self.soup.select(title_body_parent_selector)[0].select("p")
        # _ = self.get_from_selector(
        #     title_body_parent_selector,
        #     title_body_selector,
        #     get="text",
        # )

        link_parent_selector = "td[headers='archive_title']"
        link_selector = "a"
        # _ = self.get_from_selector(link_parent_selector, link_selector, get="href")

        author_parent_selector = parent_selector
        author_selector = "td[headers='archive_auth']"
        # _ = self.get_from_selector(author_parent_selector, author_selector, get="text")

        logging.debug("starting to scrape paginated urls")
        paginate_filter_and_save_data(
            output_manager,
            site_url,
            url_id,
            paginator,
            match_keywords,
            title_selector=(title_parent_selector, title_selector),
            link_selector=(link_parent_selector, link_selector),
            title_body_selector=(title_body_parent_selector, title_body_selector),
            date_selector=(date_parent_selector, date_selector),
            author_selector=(author_parent_selector, author_selector),
            from_date=from_date,
            to_date=to_date,
            visit_to_get=["body"],
            kill_thread=kill_thread,
            # timeout=5,
        )
        # set(
        #     map(
        #         scrapped_data.extend,
        #         (
        #             _data
        #             for _data in [
        #                 list(data) for data in scrapped_generator_data.values()
        #             ]
        #         ),
        #     )
        # )
    except Exception as e:
        logging.error(f"Error while scrapping paginated data: {e}")
    # return scrapped_data
    return


def search_morningstar_investment_trust_research(
    output_manager, url, url_id, from_date, to_date, kill_thread
):
    logging.debug("Starting search for search_morningstar_fund_research")
    # date format YYYY-MM-DD

    try:
        market_url = "https://www.morningstar.co.uk/uk/collection/2135/investment-trust-research--insights.aspx"
        market_url_parser = UrlParser(market_url, timeout=5)
        assert market_url_parser.soup, f"Failed to load url: {url}"
        logging.debug("market_url loaded successfully")
        self = market_url_parser
        parent_next_page_selector = None
        next_page_selector = None
        # self.get_from_selector(
        #     parent_next_page_selector, next_page_selector, get="href"
        # )
        # parent_max_page_selector = None
        # max_page_selector = None

        # max_page_regex = "Page (.*)?"
        logging.debug("Creating paginator")
        paginator = market_url_parser.get_paginator(
            get_next_page_selector=(parent_next_page_selector, next_page_selector),
            get_max_page_selector=None,
            max_page_regex=None,
        )
        # next(paginator)
    except Exception as e:
        logging.error(f"Error while generating paginator: {e}")

    try:
        logging.debug("configuring selectors for feed extraction")

        match_keywords = partial(keywords_manager.match_keywords, url)
        site_url = url

        # self = market_url_parser
        parent_selector = None
        date_parent_selector = parent_selector
        date_selector = "td[headers='archive_date']"
        # _ = self.soup.select(date_parent_selector)
        # len(_)
        # _ = self.get_from_selector(date_parent_selector, date_selector, get="text")
        # self.url

        title_parent_selector = parent_selector
        title_selector = "td[headers='archive_title']"
        # _ = self.get_from_selector(title_parent_selector, title_selector, get="text")

        title_body_parent_selector = None
        title_body_selector = ".seopurpose h2, .seopurpose p"
        # self.soup.select(title_body_parent_selector)[0].select("p")
        # _ = self.get_from_selector(
        #     title_body_parent_selector,
        #     title_body_selector,
        #     get="text",
        # )

        link_parent_selector = "td[headers='archive_title']"
        link_selector = "a"
        # _ = self.get_from_selector(link_parent_selector, link_selector, get="href")

        author_parent_selector = parent_selector
        author_selector = "td[headers='archive_auth']"
        # _ = self.get_from_selector(author_parent_selector, author_selector, get="text")

        logging.debug("starting to scrape paginated urls")
        paginate_filter_and_save_data(
            output_manager,
            site_url,
            url_id,
            paginator,
            match_keywords,
            title_selector=(title_parent_selector, title_selector),
            link_selector=(link_parent_selector, link_selector),
            title_body_selector=(title_body_parent_selector, title_body_selector),
            date_selector=(date_parent_selector, date_selector),
            author_selector=(author_parent_selector, author_selector),
            from_date=from_date,
            to_date=to_date,
            visit_to_get=["body"],
            kill_thread=kill_thread,
            # timeout=5,
        )
        # set(
        #     map(
        #         scrapped_data.extend,
        #         (
        #             _data
        #             for _data in [
        #                 list(data) for data in scrapped_generator_data.values()
        #             ]
        #         ),
        #     )
        # )
    except Exception as e:
        logging.error(f"Error while scrapping paginated data: {e}")
    # return scrapped_data
    return


def bestinvest(output_manager, url, url_id, from_date, to_date, kill_thread):
    logging.debug("Starting search for search_morningstar_fund_research")
    # date format YYYY-MM-DD

    try:
        market_url = "https://www.bestinvest.co.uk/news/investing/1"
        market_url_parser = UrlParser(market_url, timeout=5)
        assert market_url_parser.soup, f"Failed to load url: {url}"
        logging.debug("market_url loaded successfully")
        self = market_url_parser
        parent_next_page_selector = None
        next_page_selector = None
        # self.get_from_selector(
        #     parent_next_page_selector, next_page_selector, get="href"
        # )
        # parent_max_page_selector = None
        # max_page_selector = None

        # max_page_regex = "Page (.*)?"
        logging.debug("Creating paginator")
        paginator = market_url_parser.get_paginator(
            get_next_page_selector=(parent_next_page_selector, next_page_selector),
            get_max_page_selector=None,
            max_page_regex=None,
        )
        # next(paginator)
    except Exception as e:
        logging.error(f"Error while generating paginator: {e}")

    try:
        logging.debug("configuring selectors for feed extraction")

        match_keywords = partial(keywords_manager.match_keywords, url)
        site_url = url

        # self = market_url_parser
        parent_selector = None
        date_parent_selector = parent_selector
        date_selector = ".jLomYA"
        # -- getter
        # lambda x:x.split('|')[0].strip()
        # _ = self.soup.select(date_parent_selector)
        # len(_)
        # _ = self.get_from_selector(date_parent_selector, date_selector, get=lambda x:x.split('|')[0].strip())
        # self.url

        title_parent_selector = 'div[data-test="ArticleCard"]'
        title_selector = "h4"
        # _ = self.get_from_selector(title_parent_selector, title_selector, get="text")

        title_body_parent_selector = None
        title_body_selector = ".RteTextRenderer-root"
        # self.soup.select(title_body_parent_selector)[0].select("p")
        # _ = self.get_from_selector(
        #     title_body_parent_selector,
        #     title_body_selector,
        #     get="text",
        # )

        link_parent_selector = title_parent_selector
        link_selector = "a"
        _ = self.get_from_selector(link_parent_selector, link_selector, get="href")

        author_parent_selector = parent_selector
        author_selector = 'span:-soup-contains("Written by")'
        # _ = self.get_from_selector(author_parent_selector, author_selector, get=lambda x:x.replace("Written by",""))

        logging.debug("starting to scrape paginated urls")
        paginate_filter_and_save_data(
            output_manager,
            site_url,
            url_id,
            paginator,
            match_keywords,
            title_selector=(title_parent_selector, title_selector),
            link_selector=(link_parent_selector, link_selector),
            title_body_selector=(title_body_parent_selector, title_body_selector),
            date_selector=(date_parent_selector, date_selector),
            author_selector=(author_parent_selector, author_selector),
            from_date=from_date,
            to_date=to_date,
            visit_to_get=["body", "author"],
            kill_thread=kill_thread,
            # timeout=5,
        )
        # set(
        #     map(
        #         scrapped_data.extend,
        #         (
        #             _data
        #             for _data in [
        #                 list(data) for data in scrapped_generator_data.values()
        #             ]
        #         ),
        #     )
        # )
    except Exception as e:
        logging.error(f"Error while scrapping paginated data: {e}")
    # return scrapped_data
    return


def this_is_money(output_manager, site_url, url_id, from_date, to_date, kill_thread):
    try:
        from_date = datetime.strptime(from_date, "%Y-%m-%d")
        to_date = datetime.strptime(to_date, "%Y-%m-%d")
        current_date = from_date
        titles = []
        bodies = []
        title_links = []
        title_dates = []
        authors = []
        while current_date <= to_date:
            logging.info(f"Searching thisismoney for date: {current_date}")
            print(f"Searching thisismoney for date: {current_date}")
            xml_url = f'https://www.thisismoney.co.uk/sitemap-articles-day~{current_date.strftime("%Y-%m-%d")}.xml'
            parser = XMLParser(xml_url)
            for url in parser.root_element.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
                article_link = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
                print(f"searching for url: {article_link}")
                lastmod = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod').text
                try:
                    lastmod = datetime.strptime(lastmod,"%Y-%m-%dT%H:%M:%SZ")
                except Exception as e:
                    logging.error(f"Error converting lastmod to datetime: {lastmod} for thisismoney url")
                if not article_link:continue
                parsed_article = UrlParser(article_link)
                title_links.append(article_link)
                # Title
                title = parsed_article.get_from_selector(selector='h1')
                if len(title)==1:
                    titles.extend(title)
                else:
                    titles.append(";".join(title))
                # Body
                body = parsed_article.get_from_selector(selector='[itemprop="articleBody"]')
                if len(body)==1:
                    bodies.extend(body)
                else:
                    bodies.append(";".join(body))
                # title_date
                author = parsed_article.get_from_selector(selector='.author')
                if len(author)==1:
                    authors.extend(author)
                else:
                    authors.append(",".join(author))
                title_dates.append(lastmod)

            current_date += timedelta(days=1)
        page_data = zip(titles, bodies, title_links, title_dates, authors)
        title_body_decode = lambda matched_by_title, matched_by_body: (
                "",
                matched_by_title,
                matched_by_body,
            )
        match_keywords = partial(keywords_manager.match_keywords, site_url)
        just_save_data(page_data,site_url,output_manager, site_url, url_id, from_date, to_date, title_body_decode,site_url,match_keywords,1)
    
    except Exception as e:
        logging.error(f"Error while scraping this_is_money: {e}")

def moneytothemasses(output_manager, url, url_id, from_date, to_date, kill_thread):
    logging.debug("Starting search for search_morningstar_fund_research")
    # date format YYYY-MM-DD

    try:
        market_url = "https://moneytothemasses.com/category/news/page/1"
        market_url_parser = UrlParser(market_url, timeout=5)
        assert market_url_parser.soup, f"Failed to load url: {url}"
        logging.debug("market_url loaded successfully")
        self = market_url_parser
        parent_next_page_selector = None
        next_page_selector = None
        # self.get_from_selector(
        #     parent_next_page_selector, next_page_selector, get="href"
        # )
        # parent_max_page_selector = None
        # max_page_selector = None

        # max_page_regex = "Page (.*)?"
        logging.debug("Creating paginator")
        paginator = market_url_parser.get_paginator(
            get_next_page_selector=(parent_next_page_selector, next_page_selector),
            get_max_page_selector=None,
            max_page_regex=None,
        )
        # next(paginator)
    except Exception as e:
        logging.error(f"Error while generating paginator: {e}")

    try:
        logging.debug("configuring selectors for feed extraction")

        match_keywords = partial(keywords_manager.match_keywords, url)
        site_url = url

        # self = market_url_parser
        parent_selector = None
        date_parent_selector = parent_selector
        date_selector = ".date"
        # _ = self.get_from_selector(date_parent_selector, date_selector)
        # self.url

        title_parent_selector = None
        title_selector = "h3 a"
        # _ = self.get_from_selector(title_parent_selector, title_selector, get="text")

        title_body_parent_selector = None
        title_body_selector = "article"
        # self.soup.select(title_body_parent_selector)[0].select("p")
        # _ = self.get_from_selector(
        #     title_body_parent_selector,
        #     title_body_selector,
        #     get="text",
        # )

        link_parent_selector = None
        link_selector = "h3 a"
        # _ = self.get_from_selector(link_parent_selector, link_selector, get="href")

        author_parent_selector = parent_selector
        author_selector = '.author'
        # _ = self.get_from_selector(author_parent_selector, author_selector)

        logging.debug("starting to scrape paginated urls")
        paginate_filter_and_save_data(
            output_manager,
            site_url,
            url_id,
            paginator,
            match_keywords,
            title_selector=(title_parent_selector, title_selector),
            link_selector=(link_parent_selector, link_selector),
            title_body_selector=(title_body_parent_selector, title_body_selector),
            date_selector=(date_parent_selector, date_selector),
            author_selector=(author_parent_selector, author_selector),
            from_date=from_date,
            to_date=to_date,
            visit_to_get=["body"],
            kill_thread=kill_thread,
            # timeout=5,
        )
        # set(
        #     map(
        #         scrapped_data.extend,
        #         (
        #             _data
        #             for _data in [
        #                 list(data) for data in scrapped_generator_data.values()
        #             ]
        #         ),
        #     )
        # )
    except Exception as e:
        logging.error(f"Error while scrapping paginated data: {e}")
    # return scrapped_data
    return

@calculate_time_taken
def search_manager(
    url: str,
    url_id,
    output_manager: OutputManager,
    from_date: str,
    to_date: str,
    kill_thread: list,
):
    try:
        match url:
            case "https://www.ft.com/markets":
                search_ft_markets(
                    output_manager, url, url_id, from_date, to_date, kill_thread
                )
            case "https://www.cityam.com/category/markets/":
                search_cityam_markets(
                    output_manager, url, url_id, from_date, to_date, kill_thread
                )
            case "https://www.reuters.com/markets/funds/":
                search_reuters_fundnews(
                    output_manager, url, url_id, from_date, to_date, kill_thread
                )
            case "https://www.hl.co.uk/news/tags/funds":
                search_hargreaves_lansdown_funds(
                    output_manager, url, url_id, from_date, to_date, kill_thread
                )
            case "https://www.investmentweek.co.uk/category/investment/funds":
                search_investmentweek_funds(
                    output_manager, url, url_id, from_date, to_date, kill_thread
                )
            case "https://www.morningstar.co.uk/uk/collection/2114/fund-research--insights.aspx?page=1":
                search_morningstar_fund_research(
                    output_manager, url, url_id, from_date, to_date, kill_thread
                )
            case "https://www.etfstream.com/news":
                search_etfstream(
                    output_manager, url, url_id, from_date, to_date, kill_thread
                )
            case "https://www.morningstar.co.uk/uk/collection/2135/investment-trust-research--insights.aspx":
                search_morningstar_investment_trust_research(
                    output_manager, url, url_id, from_date, to_date, kill_thread
                )
            case "https://www.bestinvest.co.uk/news/investing":
                bestinvest(output_manager, url, url_id, from_date, to_date, kill_thread)
            case "https://www.thisismoney.co.uk/money/investing/index.html":
                this_is_money(
                    output_manager, url, url_id, from_date, to_date, kill_thread
                )
            case "https://moneytothemasses.com/category/news":
                moneytothemasses(output_manager, url, url_id, from_date, to_date, kill_thread)
            case _:
                logging.warning(f"Support for URL: {url} has not yet added")
                return
        logging.debug(f"Saving data for {url} in {output_manager.staging_folder_path}")
    except Exception as e:
        logging.error(f"error while processing url: {url}: \n{e}")
    # output_manager.append_file(url_id, data)


@calculate_time_taken
def main(search_params, kill_thread=[], shared=None):
    futures = []
    session_id = uuid4()
    output_manager = OutputManager(configs, session_id)
    keywords_manager.load_keywords()
    # print(1, search_params)
    for url_id, params in enumerate(search_params):
        print(params)
        url, from_date, to_date = params
        logging.debug(f"Url {url} submitted to search manager")
        futures.append(
            thread_executor.submit(
                search_manager,
                url,
                url_id,
                output_manager,
                from_date,
                to_date,
                kill_thread,
            )
        )
    wait(futures, timeout=10000)
    if not kill_thread:
        processed_session_fname = output_manager.save_session_file()
        if shared:
            shared["latest_file_path"] = processed_session_fname
