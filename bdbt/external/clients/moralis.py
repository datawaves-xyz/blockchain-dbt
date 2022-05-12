import logging
import time
from typing import Optional, Dict, List

from requests import RequestException

from bdbt.utils import execute_with_retries, get_url_retry_on_failed


class Moralis:
    def __init__(
            self, host: str, max_tokens: int = 30000
    ) -> None:
        self.host = host
        self.logger = logging.getLogger(self.__class__.__name__)
        self.max_tokens = max_tokens

    def drink_up_single_token_metadata(
            self, address: str, api_key: str
    ) -> List[Dict[str, any]]:
        results = []
        cursor = None

        while cursor != '':
            res = execute_with_retries(self._get_token_metadata, address, api_key, cursor)

            if res['total'] > self.max_tokens:
                self.logger.info(f'the total of {address} more than {self.max_tokens}, skip it')
                return []

            cursor = res['cursor']
            results.extend(res['result'])
            self.logger.debug(len(results))
            time.sleep(1)

        return results

    def _get_token_metadata(
            self, address: str, api_key: str, cursor: Optional[str] = None
    ) -> Dict[str, any]:
        params = {
            'chain': 'eth',
            'format': 'decimal',
            'limit': 500
        }

        if cursor is not None:
            params['cursor'] = cursor

        res = get_url_retry_on_failed(
            url=f'{self.host}/nft/{address}',
            params=params,
            headers={
                'x-api-key': api_key
            }
        )

        if not str(res.status_code).startswith('2'):
            log_msg = f'request failed, response: {res.text}, url: {res.url}, api_key: {api_key}'
            self.logger.warning(log_msg)
            raise RequestException(log_msg)

        return res.json()
