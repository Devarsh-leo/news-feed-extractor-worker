import time
import functools
import logging
import os


def validate_configs(configs: dict):
    for k, v in configs.items():
        if isinstance(v, str):
            if v.startswith("./"):
                if not os.path.exists(v):
                    os.makedirs(v, exist_ok=True)
                    logging.info(
                        f"Key: {k} - Creating path {os.path.join(os.getcwd(),v.lstrip('./'))}"
                    )


def calculate_time_taken(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        seconds_taken = end_time - start_time
        minutes_taken, seconds_taken = divmod(seconds_taken, 60)
        hours_taken, minutes_taken = divmod(minutes_taken, 60)
        logging.info(
            f"Time taken: {hours_taken}h:{minutes_taken}m:{seconds_taken:.2f}s"
        )
        return result

    return wrapper
