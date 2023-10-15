from bs4 import BeautifulSoup
import logging
import time
import re
import requests
from urllib.parse import urlparse, urljoin, parse_qs, urlencode, urlunparse


class UrlParser:
    __slots__ = (
        "url",
        "expected_status_code",
        "soup",
        "max_page_limits",
        "max_retries",
        "default_max_page",
        "timeout",
        "api_limit_delay",
        "max_child_depth",
    )

    def __init__(self, url, expected_status_code=200, max_retries=3, timeout=3):
        self.url = url
        self.expected_status_code = expected_status_code
        self.max_page_limits = {
            "www.ft.com": 40,
            "www.reuters.com": 3277,
            "www.cityam.com": 1028,
        }
        self.max_retries = max_retries
        self.default_max_page = 20
        self.timeout = timeout
        self.api_limit_delay = 30
        self.soup = self._load()
        self.max_child_depth = 5

    def _load(self):
        retries = 0

        while retries < self.max_retries:
            try:
                response = requests.get(self.url, timeout=self.timeout)
                if response.status_code == self.expected_status_code:
                    return BeautifulSoup(response.text, "html.parser")
                elif response.status_code == 429:
                    logging.warning(
                        f"Retrying after {self.api_limit_delay} seconds ({retries}/{self.max_retries})"
                    )
                    time.sleep(self.api_limit_delay)
                    self.api_limit_delay *= 5
                    retries += 1
                    continue
                else:
                    logging.error(f"Invalid status_code: {response.status_code}")
                    return
            except requests.exceptions.Timeout as e:
                logging.error(str(e))

            except Exception as e:
                logging.error(str(e))
                logging.info(f"will retry after: {self.api_limit_delay} seconds...")
                # time.sleep(self.api_limit_delay)
                # self.api_limit_delay *= 5

            retries += 1
            if retries < self.max_retries:
                logging.warning(f"Retrying... ({retries}/{self.max_retries})")

        logging.error("Max retries reached. Giving up.")
        return None

    def _validate_find_all(self, data, many, expected):
        len_data = len(data)
        if not data:
            assert False, "No elemnent was located"
        if not many:
            assert (
                len_data == 1
            ), f"{len_data} elements are located, expected only 1 element"

    def _get_from_element(self, tags, get):
        # if isinstance(get,dict):
        #     key , argument = next(iter(get.items()))
        # else:
        #     key = 'text'

        match get:
            case "text":
                return [tag.get_text().strip() if tag else "" for tag in tags]

            case "href":
                return [
                    (
                        tag.get("href", "").strip()
                        if urlparse(tag.get("href")).netloc
                        else urljoin(self.url, tag.get("href"))
                    )
                    if tag
                    else ""
                    for tag in tags
                ]
            case None:
                return tags
            case _:
                return [
                    str(tag.get(get)).strip("None").strip() if tag else ""
                    for tag in tags
                ]

    def _construct_next_page_url(self, parsed_url, query_params, current_page):
        updated_url = ""
        if parsed_url.netloc in {
            "www.ft.com",
            "www.reuters.com",
            "www.morningstar.co.uk",
            "www.hl.co.uk",
        }:
            # Update the 'page' parameter in the query parameters
            if parsed_url.netloc == "www.hl.co.uk":
                query_params["result_15077628_result_page"] = [str(current_page)]
            else:
                query_params["page"] = [str(current_page)]
            updated_url = urlunparse(
                (
                    parsed_url.scheme,
                    parsed_url.netloc,
                    parsed_url.path,
                    parsed_url.params,
                    urlencode(
                        query_params, doseq=True
                    ),  # doseq=True for multiple values with the same key
                    parsed_url.fragment,
                )
            )
        elif parsed_url.netloc in {"www.cityam.com", "www.investmentweek.co.uk"}:
            updated_url = re.sub(
                "page/([0-9]*)?", f"page/{str(current_page)}", parsed_url.geturl()
            )
        return updated_url

    def _generate_next_urls(self, next_page_url, current_page=1, max_page=10000):
        # Parse the URL
        parsed_url = urlparse(next_page_url)
        # Parse the query parameters
        query_params = parse_qs(parsed_url.query)

        while current_page <= max_page:
            logging.debug(f"creating url for page: {current_page}")
            updated_url = self._construct_next_page_url(
                parsed_url, query_params, current_page
            )
            assert updated_url, "Please add support to create updated_url"
            yield current_page, updated_url

            current_page += 1
        logging.debug(f"Breaking pagination url generation on {current_page}")

    def get_from_selector(self, parent_selector=None, selector=None, get="text"):
        if parent_selector:
            parent = self.soup.select(parent_selector)
            tags = [i.select_one(selector) for i in parent]
        else:
            tags = self.soup.select(selector)
        return self._get_from_element(tags, get)
        # match get:
        #     case "text":
        #         return [elem.get_text().strip() if elem else "" for elem in tags]
        #     case None:
        #         return tags
        #     case _:
        #         return [
        #             elem.get(get).strip() if elem and elem.get(get) else ""
        #             for elem in tags
        #         ]

    def find_all(
        self,
        selector,
        many=True,
        expected=None,
        get="text",
        #   from_parent_by=None
    ):
        tags = self.soup.select(selector)
        # if from_parent_by:
        #     parent = self.soup.find(**from_parent_by)
        #     tags = parent.find_all(**by)
        # else:
        #     tags = self.soup.find_all(**by)
        # depth = 0
        # tags = self.soup
        # next_by = None
        # many_tags = None
        # by = by.copy()
        # while depth < 5:  # self.max_child_depth:
        #     depth += 1
        #     if "next" in by:
        #         next_by = by["next"]
        #         del by["next"]
        #     if "many" in by:
        #         many_tags = True
        #         del by["many"]

        #     if next_by:
        #         if many_tags:
        #             tags = self.soup.find_all(**by)
        #         else:
        #             tags = self.soup.find(**by)
        #         by = next_by
        #         next_by = None
        #         many_tags = None
        #     else:
        #         if isinstance(tags, list):
        #             tags = [tag.find_all(**by)[0] for tag in tags]
        #         else:
        #             tags = tags.find_all(**by)
        #         break

        self._validate_find_all(tags, many, expected)
        data = self._get_from_element(tags, get)

        return data[0] if not many else data

    def get_paginator(
        self, get_next_page_selector, get_max_page_selector, max_page_regex=None
    ):
        logging.debug("extracting next_page_url and max_pages")
        if isinstance(get_next_page_selector, tuple) and any(get_next_page_selector):
            next_page_url = self.get_from_selector(*get_next_page_selector, get="href")
            if isinstance(next_page_url, list) and len(next_page_url) == 1:
                next_page_url = next_page_url[0]
        else:
            next_page_url = self.url
        if get_max_page_selector:
            try:
                max_pages = self.get_from_selector(*get_max_page_selector, get="text")
                if len(max_pages) > 1:
                    max_numbers = set()
                    if max_page_regex:
                        for number in max_pages:
                            try:
                                number = re.findall(max_page_regex, number)[0]
                                max_numbers.add(number)
                            except Exception as e:
                                print(e)
                        max_pages = max_numbers

                    max_page = max(
                        (
                            int(
                                re.sub(r"[^0-9]", "", number)
                                if re.sub(r"[^0-9]", "", number)
                                else 0
                            )
                            for number in max_pages
                        )
                    )
                else:
                    if max_page_regex:
                        max_page = re.findall(max_page_regex, max_pages[0])[0]
                    else:
                        max_page = max_pages[0]
            except Exception as e:
                logging.error(
                    f"Error while selecting max_page using {get_max_page_selector} setting to default: 50"
                )

            logging.debug(
                f"max_page located: {max_page}, next_page_url: {next_page_url}"
            )
            try:
                max_page = int(max_page)
            except Exception as e:
                max_page = self.default_max_page
                logging.error(
                    f"Failed to convert max page to int: {max_page}, Max Pages fetched will be {self.default_max_page}"
                )
        else:
            max_page = self.max_page_limits.get(urlparse(self.url).netloc, 1000)
            logging.info(f"Setting max_page to {max_page}")
        # Inform when search results have more pages than the platform supports to fetch per query
        if self.max_page_limits.get(urlparse(self.url).netloc) and (
            max_page > self.max_page_limits[urlparse(self.url).netloc]
        ):
            logging.warning(
                f"Maximum pages per query are {self.max_page_limits[urlparse(self.url).netloc]}, Please shorten the date-range to reduce the query result."
            )
            max_page = self.max_page_limits[urlparse(self.url).netloc]

        return self._generate_next_urls(next_page_url, max_page=max_page)
