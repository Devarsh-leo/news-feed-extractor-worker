import os
import logging
import json
import re

class KeywordsManager:
    def __init__(self, configs):
        self.keywords_path = configs["url_keywords_path"]
        self.keywords = None
        self.load_keywords()

    def load_keywords(self) -> None:
        try:
            if os.path.exists(self.keywords_path):
                # keywords_path = 'configs.json'
                with open(self.keywords_path, "r") as fp:
                    data = json.load(fp)
                    assert isinstance(
                        data, dict
                    ), f"{self.keywords_path} file has been corrupted, data is not of type dict but: {type(data)}"
                    self.keywords = {
                        url: set(
                            keyword.lower()
                            for keyword, isChecked in payload.get(
                                "keywords", {}
                            ).items()
                            if isChecked == True
                        )
                        for url, payload in data.items()
                    }
            else:
                logging.error(f"Keywords file not found at path: {self.keywords_path}")
        except Exception as e:
            logging.error(f"Failed to load keywords at {self.keywords_path}: {e}")

    def get_keywords(self, url: str) -> set:
        try:
            if url in self.keywords:
                return self.keywords[url]
            else:
                logging.warning(f"URL: {url} not present in {self.keywords_path}")
        except Exception as e:
            logging.error(f"Failed to get keyword: {e}")

        return set()
    def match_exact_keyword(self,keyword, text):
        pattern = r'\b' + re.escape(keyword.lower()) + r'\b'
        match = re.search(pattern, text.lower())
        if match:
            return True
        else:
            return False
    
    def match_keywords(self, url: str, title: str):
        if not self.keywords:
            self.keywords = self.load_keywords()
        assert url in self.keywords, f"keywords list not configured for {url}"
        return ":".join(
            [keyword for keyword in self.keywords[url] if self.match_exact_keyword(keyword, title)]
        )

    def update_all_keywords(self, data: dict) -> None:
        try:
            assert isinstance(
                data, dict
            ), f"unable to update keywords, data is not of type dict but: {type(data)}"
            with open(self.keywords_path, "w") as fp:
                json.dump(data, fp, indent=3)
        except Exception as e:
            logging.error(f"Failed to update all keywords at {self.keywords_path}: {e}")
