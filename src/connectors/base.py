from typing import Any, Protocol


class SourceConnector(Protocol):
    # Generic connector contract (works for scraper/api/file sources)
    def extract(
        self,
        max_urls_counter: int,
        max_data_length: int,
        timeout: int,
    ) -> list[dict[str, Any]]: ...
