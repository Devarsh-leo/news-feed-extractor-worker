from modules.flows import main
import logging

logging.basicConfig(level=logging.DEBUG, filename="app.log", filemode="w")
from_date = "2023-10-10"
to_date = "2023-10-10"
# session_id = uuid4()
search_params = [
    # ["https://www.ft.com/markets", "2023-10-10", "2023-10-10"],
    # ["https://www.cityam.com/category/markets/", "2023-10-10", "2023-10-10"],
    ["https://www.reuters.com/news/archive/fundsFundsNews", "2023-10-10", "2023-10-10"],
    # ["https://www.hl.co.uk/news/tags/funds", "2023-10-10", "2023-10-10"],
    # [
    #     "https://www.investmentweek.co.uk/category/investment/funds",
    #     "2023-10-10",
    #     "2023-10-10",
    # ],
    # [
    #     "https://www.morningstar.co.uk/uk/collection/2114/fund-research--insights.aspx?page=1",
    #     "2023-10-10",
    #     "2023-10-10",
    # ],
]

kill_thread = []


def extract(search_params, kill_thread):
    main(search_params, kill_thread=kill_thread)
    # logging.info(
    # f"Completed Search for sessionId: {session_id} for date range {from_date} - {to_date}"
    # )


if __name__ == "__main__":
    extract(search_params, kill_thread)
