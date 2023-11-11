from .url_parser import UrlParser

# from .keywords_manager import KeywordsManager
from .output_manager import OutputManager
import time
import logging
from datetime import datetime , timedelta
import sys


# import pytz

date_string_format = {
    "https://www.ft.com/markets": "%B %d, %Y",
    "https://www.cityam.com/category/markets/": "%B %d, %Y",
    "https://www.reuters.com/news/archive/fundsFundsNews": ("%b %d %Y", "%H:%M%p EDT","%H:%M%p EST"),
    "https://www.hl.co.uk/news/tags/funds": "%d %b %Y",
    "https://www.investmentweek.co.uk/category/investment/funds": "%d %B %Y",
    "https://www.morningstar.co.uk/uk/collection/2114/fund-research--insights.aspx?page=1": "%d/%m/%y",
}
date_output_format = "%B %d, %Y"
# edt_timezone = pytz.timezone("America/New_York")


def todays_time(
    _datetime: datetime = None, hour=None, minute=None, seconds=None, tz=None
):
    today = datetime.now(tz)
    if not hour:
        hour = _datetime.hour if isinstance(_datetime, datetime) else today.hour
    if not minute:
        minute = _datetime.minute if isinstance(_datetime, datetime) else today.minute
    if not seconds:
        seconds = _datetime.second if isinstance(_datetime, datetime) else today.second
    return datetime(
        year=today.year,
        month=today.month,
        day=today.day,
        hour=int(hour),
        minute=int(minute),
        second=int(seconds),
    )


def paginate_filter_and_save_data(
    output_manager: OutputManager,
    site_url,
    url_id,
    paginator,
    match_keywords,
    title_selector,
    link_selector,
    title_body_selector,
    date_selector,
    author_selector,
    from_date,
    to_date,
    visit_to_get=[],
    timeout=3,
    kill_thread=[],
):
    logging.debug("converting from_date,to_date to date_time")
    from_date = datetime.strptime(from_date, "%Y-%m-%d")
    to_date = datetime.strptime(to_date, "%Y-%m-%d")
    logging.debug(f"from_date: {from_date}, to_date: {to_date}")

    def get_datetime(str_date):
        try:
            # logging.debug("transforming date to datetime")
            if site_url not in date_string_format:
                logging.error(f"strint format unknown for {site_url} ")
                return
            elif not str_date:
                return
            if "|" in str_date:
                str_date = str_date.split("|")[0].strip()
            if "•" in str_date:
                str_date = str_date.split("•")[0].strip()
            if isinstance(date_string_format[site_url], tuple):
                for format in date_string_format[site_url]:
                    try:
                        date = datetime.strptime(str_date.strip(), format)
                        break
                    except:
                        pass
            else:
                date = datetime.strptime(str_date.strip(), date_string_format[site_url])
            return repair(date)
        except Exception as e:
            logging.error(f"Error in get_datetime, {e} for {site_url} value: {str_date}")

    def repair(date):
        match site_url:
            case "https://www.reuters.com/news/archive/fundsFundsNews":
                if date.year == 1900:
                    date = todays_time(date)
        return date
    
    def transform_date_to_output_format(i_date):
        # logging.debug("Transform date to output format")
        try:
            if i_date:
                date = get_datetime(i_date)
                if date:
                    
                    date = date.strftime(date_output_format)
                else:
                    logging.warning(f"No date value after get it from get_datetime func! for date: {i_date}")
            else:
                logging.warning("Date is empty")
        except Exception as e:
            logging.error(f"Error {e}")
            raise e
        return date

    # print(1)
    # current_page, url = next(paginator)
    end_pagination = None
    for current_page, url in paginator:
        if kill_thread:
            logging.error("Killing the thread")
            sys.exit()
        try:
            logging.info(f"Page: {current_page} , {url}")
            time.sleep(0.01)
            paginated_url_parser = UrlParser(url, timeout=timeout)
            assert paginated_url_parser.soup, f"Failed to load url: {url}"
            # 3
            title_date = paginated_url_parser.get_from_selector(
                *date_selector,
                get="text",
                # from_parent_by=from_title_container_by
            )
            # print(2, title_date[-1])
            title_date_dt = get_datetime(title_date[-1])
            # print(3)
            # if title_date_dt < from_date:
            #     continue
            if title_date_dt and title_date_dt < from_date:
                logging.info(
                    f"will stop the loop for {url} as max_title_date:{title_date_dt}< to_date:{from_date} for page result"
                )
                end_pagination = True
            logging.debug(f"feed last date: {title_date_dt}")
            # 0
            title = paginated_url_parser.get_from_selector(
                *title_selector,
                get="text",
                # from_parent_by=from_title_container_by,
            )
            # 1
            if "body" not in visit_to_get:
                partial_body = paginated_url_parser.get_from_selector(
                    *title_body_selector,
                    get="text",
                    # from_parent_by=from_title_container_by,
                )
            # 2
            title_links = paginated_url_parser.get_from_selector(
                *link_selector,
                get="href",
                # from_parent_by=from_title_container_by,
            )

            # 4
            if "author" not in visit_to_get:
                author = paginated_url_parser.get_from_selector(
                    *author_selector,
                    get="text",
                    #   from_parent_by=from_title_container_by
                )

            resp_body, resp_author = visit_page_and_get_data(
                visit_to_get,
                site_url,
                title_links,
                title_body_selector,
                author_selector,
                timeout,
                kill_thread,
            )
            if "body" in visit_to_get:
                partial_body = resp_body
            if "author" in visit_to_get:
                author = resp_author

            # logging.debug(f"Partial body: {partial_body}, author: {author}")

            logging.debug(
                f"title: {len(title)}, body: {len(partial_body)}, date: {len(title_date)}, links: {len(title_links)}, author: {len(author)}"
            )
            if not (len(title) == len(title_links) == len(title_date)):
                logging.warning(
                    f"Mismatch detected in no of extracted title: {len(title)}, links: {len(title_links)} and dates: {len(title_date)}"
                )
                no_of_records = min(len(title), len(title_links), len(title_date))
            else:
                no_of_records = len(title)
            # if not author:
            #     author = (None for i in range(no_of_records))

            if paginated_url_parser.soup:
                page_data = zip(title, partial_body, title_links, title_date, author)
            else:
                # on request timed out reached limit
                continue
            # function
            title_body_decode = lambda matched_by_title, matched_by_body: (
                "",
                matched_by_title,
                matched_by_body,
            )
            # logging.debug(
            #     f"Page_data {url}: {title, partial_body, title_links, title_date, author}"
            # )
            filtered_data = []
            # data = next(page_data)
            for data in page_data:
                data_date = get_datetime(data[3])
                if (match_keywords(data[0]) or match_keywords(data[1])):
                    if (
                        0<= (data_date - to_date).seconds < 86400 and
                        from_date <= data_date <= to_date+timedelta(days=1)
                        ) or (
                            from_date <= data_date <= to_date
                        ) or (
                            not data_date
                        ):
                        filtered_data.append((
                            url,
                            transform_date_to_output_format(data[3]),  # Date
                            data[0],  # Title
                            data[4],  # Author
                            data[2],  # URL
                            *title_body_decode(
                                match_keywords(data[0]), match_keywords(data[1])
                            ),  # Title-Body keywords
                            site_url,  # Site
                        ))
                    
            # filtered_data = (
                # (
                #     url,
                #     transform_date_to_output_format(data[3]),  # Date
                #     data[0],  # Title
                #     data[4],  # Author
                #     data[2],  # URL
                #     *title_body_decode(
                #         match_keywords(data[0]), match_keywords(data[1])
                #     ),  # Title-Body keywords
                #     site_url,  # Site
                # )
                # for data in page_data
                # if (match_keywords(data[0]) or match_keywords(data[1]))
                # and (
                #     from_date <= get_datetime(data[3]) <= to_date
                #     if get_datetime(data[3])
                #     else True
                # )
            # )
            logger = logging.getLogger()
            if logger.getEffectiveLevel() == logging.DEBUG:
                filtered_data = list(filtered_data)
                logging.debug(
                    f"Saving data for page: {current_page}, len: {len(filtered_data)}"
                )
                logging.debug(f"Filtered data: {url}, {filtered_data} ")
            output_manager.append_file(str(url_id), filtered_data)
            if end_pagination:
                break
        except Exception as e:
            logging.error(f"error scraping data: {e} for url {url}")
    return


def visit_page_and_get_data(
    visit_to_get,
    site_url,
    title_links,
    title_body_selector,
    author_selector,
    timeout,
    kill_thread,
):
    logging.info(f"Visiting pages for: {site_url}")
    body = []
    author = []
    for url in title_links:
        if kill_thread:
            logging.error("Killing the thread")
            sys.exit()
        url_parser = UrlParser(url, timeout=timeout)
        if "body" in visit_to_get:
            text = url_parser.get_from_selector(*title_body_selector, get="text")
            body_text = ("\n".join(text) if text else "").strip()
            body.append(body_text)
            if not body_text:
                logging.warning(f"Body not located for url: {url}")
        if "author" in visit_to_get:
            article_author = url_parser.get_from_selector(*author_selector, get="text")
            author_text = (
                (", ".join(article_author) if article_author else "")
                .strip()
                .strip(", ")
            )
            author.append(author_text)
            if not author_text:
                logging.warning(f"author not located for url: {url}")
    logging.debug(f"Extracted body: {len(body)}, authors: {len(author)}")
    return body, author
