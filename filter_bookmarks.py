#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor, Future
from ssl import SSLError
from time import time
from typing import Any, Dict, Union, Generator, Tuple, List
from os.path import exists
from urllib3.exceptions import InsecureRequestWarning # type: ignore

import requests
import json
import sys
import re
import argparse

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning) # type: ignore

FILE_URL = re.compile(r'^\s*file://(.*)', re.I)

HEADERS = {
    'User-Agent': "Mozilla/5.0 (X11; Linux x86_64; rv:96.0) Gecko/20100101 Firefox/96.0"
}

def _walk_bookmarks(entry: Dict[str, Any], path: List[Dict[str, Any]]) -> Generator[Tuple[List[Dict[str, Any]], Dict[str, Any]], None, None]:
    entry_type = entry['type']
    if entry_type == 'text/x-moz-place':
        yield path, entry
    elif entry_type == 'text/x-moz-place-container':
        children = entry.get('children')
        if children is not None:
            childpath = [ *path, entry ]
            for child in children:
                yield from _walk_bookmarks(child, childpath)
    else:
        raise TypeError(f'unhandeled type: {entry_type}')

def walk_bookmarks(entry: Dict[str, Any]) -> Generator[Tuple[List[Dict[str, Any]], Dict[str, Any]], None, None]:
    yield from _walk_bookmarks(entry, [])

def filter_bookmarks(input: Dict[str, Any], max_workers: int = 64) -> Dict[str, Any]:
    url_status_futurtes: Dict[str, Future[Union[requests.Response, Exception, None]]] = {}
    drop_count = 0

    def fetch(url: str) -> Union[requests.Response, Exception, None]:
        norm_url = url.strip().lower()
        match = FILE_URL.match(url)
        if match:
            filepath = match.group(1)
            print(f'FILE {url}', file=sys.stderr)
            if exists(filepath):
                return None
            return FileNotFoundError(filepath)

        elif not norm_url.startswith('http:') and not norm_url.startswith('https:'):
            print(f'KEEP {url}', file=sys.stderr)
            return None

        print(f'FETCH {url}', file=sys.stderr)
        try:
            return requests.get(url, allow_redirects=True, verify=False, headers=HEADERS)
        except Exception as exc:
            return exc

    def _filter_bookmarks(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        filtered_entries: List[Dict[str, Any]] = []
        nonlocal drop_count

        for entry in entries:
            entry_type = entry['type']
            if entry_type == 'text/x-moz-place':
                url = entry['uri']
                result = url_status_futurtes[url].result()

                if result is None:
                    filtered_entries.append(entry)
                elif isinstance(result, SSLError):
                    # might still be an useful link, just misconfigured SSL
                    print(f'IGNORE-ERROR {type(result).__name__} {url} {result}', file=sys.stderr)
                    filtered_entries.append(entry)
                elif isinstance(result, Exception):
                    drop_count += 1
                    print(f'ERROR {type(result).__name__} {url} {result}', file=sys.stderr)
                elif (result.status_code >= 200 and result.status_code < 400) or result.status_code == 503 or result.status_code == 401 or result.status_code == 403:
                    # 503 Service Unavailable: might be temporary
                    # 401 Unauthorized: maybe need to login
                    # 403 Forbidden: maybe need to login
                    filtered_entries.append(entry)
                else:
                    drop_count += 1
                    print(f'STATUS {result.status_code} {url}', file=sys.stderr)
            else:
                filtered_entry = dict(entry)
                children = filtered_entry.get('children')
                if children is not None:
                    filtered_entry['children'] = _filter_bookmarks(children)
                filtered_entries.append(filtered_entry)

        return filtered_entries

    with ThreadPoolExecutor(max_workers) as executor:
        print('loading URLs...', file=sys.stderr)

        for path, bookmark in walk_bookmarks(input):
            url = bookmark['uri']
            if url not in url_status_futurtes:
                url_status_futurtes[url] = executor.submit(fetch, url)

        print('filtering bookmarks...', file=sys.stderr)
        filtered_entries = _filter_bookmarks([input])

    print(f'dropped {drop_count} bookmarks' if drop_count != 1 else 'dropped 1 bookmark')
    
    if not filtered_entries:
        timestamp = int(time() * 1_000_000)
        return {
            "guid": "root________",
            "id": 1,
            "index": 0,
            "type": "text/x-moz-place-container",
            "children": [],
            "root": "placesRoot",
            "title": "",
            "typeCode": 2,
            "dateAdded": timestamp,
            "lastModified": timestamp,
        }

    return filtered_entries[0]

DEFAULT_MAX_WORKERS = 2048

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--max-workers', type=int, default=DEFAULT_MAX_WORKERS, help=f'default: {DEFAULT_MAX_WORKERS}')
    ap.add_argument('bookmarks', nargs=1, help='backuped bookmarks.json file')
    ap.add_argument('output', nargs='?', default=None, help='filtered bookmarks file. default is stdout')

    args = ap.parse_args()

    with open(args.bookmarks[0], 'rb') as fin:
        input = json.load(fin)

    output = filter_bookmarks(input, args.max_workers)

    if args.output is None:
        json.dump(output, sys.stdout)
    else:
        with open(args.output, 'w') as fout:
            json.dump(output, fout)

if __name__ == '__main__':
    main()
