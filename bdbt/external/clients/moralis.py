import json
import logging
import time
from json import JSONDecodeError
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

            for data in res['result']:
                data['attributes'] = self._get_attributes(data.get('metadata')) \
                    if data.get('metadata') is not None else []

                del data['metadata']
                del data['last_token_uri_sync']
                del data['last_metadata_sync']
                results.append(data)

            cursor = res['cursor']
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

    @staticmethod
    def _get_attributes(metadata_str: Optional[str]) -> List[Dict[str, any]]:
        attributes = []

        # handle error json format
        if metadata_str.startswith('{"trait_type":') or metadata_str.startswith('{"value":'):
            metadata_str = '{"attribute":[' + metadata_str + ']}'

        try:
            metadata = json.loads(metadata_str)
            attribute_raw = metadata.get('attributes')

            if attribute_raw is None or len(attribute_raw) == 0:
                return attributes

            if type(attribute_raw) == dict:
                attribute_raw = [{
                    'trait_type': k,
                    'value': v
                } for k, v in attribute_raw.items()]

            if type(attribute_raw) != list:
                return attributes

            item = attribute_raw[0]
            if 'trait_type' not in item or 'value' not in item:
                return attributes

            attributes = [{
                'trait_type': str(i.get('trait_type')),
                'value': str(i.get('value'))
            } for i in attribute_raw if i is not None and type(i) == dict]
            return attributes
        except JSONDecodeError as e:
            logging.warning(f"can't load json str, metadata_str: {metadata_str}, error: {e}")
            return []
