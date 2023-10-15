import csv
import os
import logging
import pandas as pd
import shutil
from datetime import datetime
from .url_parser import UrlParser, urlparse


class OutputManager:
    def __init__(self, configs, session_id):
        self.output_mode = "csv"
        self.output_folder = configs["output_folder"]
        self.headers = [
            [
                "Date",
                "Title",
                "Author",
                "URL",
                "Title/Body",
                "Title Keywords",
                "Body Keywords",
                "Site",
            ]
        ]
        self.session_id = str(session_id)
        self.session_data = []
        self.staging_folder_path = os.path.join(
            configs.get("staging_folder", ""), f".{self.session_id}"
        )
        os.mkdir(self.staging_folder_path)
        self.validate()
        self.reconsilation_date_selectors_dict = {
            "www.cityam.com": (".article-header", "time", "%A %d %B %Y %I:%M %p")
        }
        self.date_output_format = "%B %d, %Y"
        self.keep_session_file = False

    def validate(self):
        assert os.path.exists(
            self.output_folder and self.staging_folder_path
        ), f"Output path does not exists {self.output_folder}"

    def load_file(self, filename: str):
        try:
            with open(filename, "r", encoding="utf-8") as csvfile:
                csvreader = csv.reader(csvfile)
                return [line for line in csvreader]

            logging.info(f"Data read from {filename}.")
        except Exception as e:
            logging.error(f"Error while reading file as {filename}: {e}")
            raise e

    def save_file(self, filename: str, data):
        try:
            fpath = os.path.join(self.output_folder, filename)
            if not fpath.endswith(".csv"):
                fpath += ".csv"
            with open(fpath, "w", newline="", encoding="utf-8") as csvfile:
                csvwriter = csv.writer(csvfile)
                if self.headers:
                    csvwriter.writerow(self.headers)
                csvwriter.writerows(data)
            logging.info(f"Data saved to {filename} successfully.")
            return True
        except Exception as e:
            logging.error(f"Error while saving file as {filename}: {e}")
            raise e

    def append_file(self, filename: str, data, staging=True):
        try:
            if staging:
                fpath = os.path.join(self.staging_folder_path, filename)
            else:
                fpath = filename
            if not fpath.endswith(".csv"):
                fpath += ".csv"
            with open(fpath, "a+", newline="", encoding="utf-8") as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerows(data)
            logging.info(f"Data successfully appended to {fpath}.")
            return True
        except Exception as e:
            logging.error(f"Error while appending data to {filename}: {e}")
            raise e

    def save_session_file(self):
        raw_name = f"raw-{self.session_id}.csv"
        processed_name = f"Extracted-Data-{self.session_id}.csv"
        raw_session_fname = os.path.join(self.output_folder, raw_name)
        processed_session_fname = os.path.join(self.output_folder, processed_name)
        self.append_file(raw_session_fname, self.headers, staging=False)
        for url_file_name in os.listdir(self.staging_folder_path):
            staging_filename = os.path.join(self.staging_folder_path, url_file_name)
            url_data = self.load_file(staging_filename)
            self.append_file(raw_session_fname, url_data, staging=False)

        shutil.rmtree(self.staging_folder_path)
        if os.path.exists(self.staging_folder_path):
            shutil.rmtree(self.staging_folder_path)
        df = pd.read_csv(raw_session_fname)
        df.drop_duplicates(inplace=True)
        agg_functions = {
            "Date": "first",
            "Title": "first",
            "Author": "first",
            "Title Keywords": lambda x: ":".join(
                (str(i) for i in set(x) if pd.notna(i))
            ),  # Concatenate with ':'
            "Body Keywords": lambda x: ":".join(
                (str(i) for i in set(x) if pd.notna(i))
            ),  # Concatenate with ', '
            # "Title/Body": "first",  # Use the first value
            "Site": "first",
            # "temp": "first",
        }
        # df.groupby(["Date", "Title", "Author", "URL", "Site"],axis=1).agg(['min'])
        columns = df.columns
        deduplicated = df.groupby(["URL"]).agg(agg_functions).reset_index()

        # reconsile date
        deduplicated["Date"] = (
            deduplicated[["Date", "URL"]]
            .fillna("")
            .apply(lambda row: self.reconsile_date(*row), axis=1)
        )

        deduplicated["Title/Body"] = deduplicated.apply(
            lambda x: "yes/yes"
            if (pd.notnull(x["Title Keywords"]) and x["Title Keywords"])
            and (pd.notnull(x["Body Keywords"] and x["Body Keywords"]))
            else (
                "yes/no"
                if (pd.notnull(x["Title Keywords"]) and x["Title Keywords"])
                else ("no/yes")
            ),
            axis=1,
        )
        deduplicated.sort_values(
            ["Date", "Site"], ascending=[False, True], inplace=True
        )

        deduplicated[columns].to_csv(processed_session_fname, index=False)
        if not self.keep_session_file:
            os.remove(raw_session_fname)
        print(f"Processed file: {processed_session_fname}")
        return processed_session_fname
        # os.remove(self.staging_folder_path)
        # update body search query in soup
        # check why duplicates in output

        # try:
        #     self.save_file(fname, self.session_data)
        # except Exception:
        #     logging.error(f"Error while saving session file {fname}:\nSaved into logs")
        #     logging.error(str(self.session_data))

    def reconsile_date(self, date, url):
        if date:
            return date
        parsed_url = urlparse(url)
        if parsed_url.netloc in self.reconsilation_date_selectors_dict:
            url_parser = UrlParser(url)
            assert url_parser.soup, "Failed to load url"
            parent_selector, selector, format = self.reconsilation_date_selectors_dict[
                parsed_url.netloc
            ]
            date = url_parser.get_from_selector(parent_selector, selector)
            if date:
                try:
                    date_time_obj = datetime.strptime(date[0], format)
                    date = date_time_obj.strftime(self.date_output_format)
                except Exception as e:
                    logging.error(
                        f"Error {e} while converting date to output_format/datetime, {date} {format} {self.date_output_format}"
                    )
                    date = ""
            else:
                date = ""
                logging.warning(f"Date not located in date reconcilation for url {url}")
        else:
            logging.warning(f"Url {url} not supported for date reconsilation.")
        return date


# self = OutputManager()
# reconsile_date(self, "", url)
