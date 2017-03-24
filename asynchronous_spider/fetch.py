# -*- coding: utf-8 -*-
import socket
import threading

import time
from Queue import Queue

import urllib.parse
import re

lock = threading.Lock()
seen_urls = {'/'}


class Fetcher(threading.Thread):
    def __init__(self, tasks):
        threading.Thread.__init__(self)

        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while 1:
            url = self.tasks.get()
            print(url)
            sock = socket.socket()
            sock.connect(('localhost', 3000))

            request = 'GET {} HTTP/1.0\r\nHost: localhost\r\n\r\n'.format(url)
            sock.send(request.encode('ascii'))
            response = b''
            chunk = sock.recv(4096)
            while chunk:
                response += chunk
                chunk = sock.recv(4096)

            links = self.parse_links(url, response)
            lock.acquire()
            for link in links.difference(seen_urls):
                self.tasks.put(link)
            seen_urls.update(links)
            lock.release()

            self.tasks.task_done()

    def parse_links(self, fetch_url, response):
        if not response:
            print('error:{}'.format(fetch_url))
            return set()
        if not self._is_html_(response):
            return set()

        urls = set(re.findall(r'''(?i)href=["']?([^\s"'<>]+)''', self.body(response)))

        links = set()
        for url in urls:
            # 可能找到的url是相对路径，这时候就需要join一下，绝对路径的话就还是会返回url
            normalized = urllib.parse.urljoin(fetch_url, url)
            # url的信息会被分段存在parts里
            parts = urllib.parse.urlparse(normalized)
            if parts.scheme not in ('', 'http', 'https'):
                continue
            host, port = urllib.parse.splitport(parts.netloc)
            if host and host.lower() not in ('localhost'):
                continue

            # 有的页面会通过地址里的#frag后缀在页面内跳转，这里去掉frag的部分
            defragmented, frag = urllib.parse.urldefrag(parts.path)
            links.add(defragmented)

        return links

    def body(self, response):
        body = response.split(b'\r\n\r\n', 1)[1]
        return body.decode('utf-8')

    def _is_html_(self, response):
        head, body = response.split(b'\r\n\r\n', 1)
        headers = dict(h.split(': ') for h in head.decode().split('\r\n')[1:])
        return headers.get('Content-Type', '').startswith('text/html')


class ThreadPool:
    def __init__(self, num_threads):
        self.tasks = Queue()
        for _ in range(num_threads):
            Fetcher(self.tasks)

    def add_task(self, url):
        self.tasks.put(url)

    def wait_completion(self):
        self.tasks.join()


if __name__ == '__main__':
    start = time.time()
    pool = ThreadPool(4)
    pool.add_task("/")
    pool.wait_completion()
    print('{} URLs fetched in {:.1f} seconds'.format(len(seen_urls), time.time() - start))
