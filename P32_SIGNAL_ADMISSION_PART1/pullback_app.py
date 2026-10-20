#!/usr/bin/env python3
"""Research-service entrypoint. No executor, webhook or order routes."""
import argparse
import json
import os
from pathlib import Path
from wsgiref.simple_server import make_server,WSGIRequestHandler
from pullback_lab import core as e
from pullback_lab.runner import Research,persistence


def application(research):
    def app(environ,start_response):
        method=environ.get('REQUEST_METHOD','GET');path=environ.get('PATH_INFO','/')
        status='200 OK';headers=[('Cache-Control','no-store')]
        if method not in ('GET','HEAD'):
            body=e.canon(dict(error='Only GET and HEAD are supported',orders_supported=False));status='405 Method Not Allowed'
        elif path in ('/','/health'):
            body=e.canon(dict(service=e.VERSION,orders_supported=False,orders_sent=False,
                              start_once='/status',result='/results.zip',protocol='/protocol',
                              output_scope='SEPARATE_PULLBACK_RESEARCH',persistence=persistence(research.directory)))
        elif path=='/protocol':
            body=e.canon(dict(signature=research.signature,**research.protocol))
        elif path=='/status':
            body=e.canon(research.status(start=method=='GET'))
        elif path=='/results.zip':
            target=research.work/'P32_PULLBACK_PASS1_RESULTS.zip'
            if target.is_file():
                headers.extend([('Content-Type','application/zip'),('Content-Disposition','attachment; filename=P32_PULLBACK_PASS1_RESULTS.zip'),('Content-Length',str(target.stat().st_size))])
                start_response(status,headers)
                if method=='HEAD':return []
                def stream():
                    with target.open('rb') as source:
                        while True:
                            block=source.read(1024*1024)
                            if not block:break
                            yield block
                return stream()
            body=e.canon(dict(error='No completed pair report yet',status='/status'));status='404 Not Found'
        else:
            body=e.canon(dict(error='Unknown route'));status='404 Not Found'
        headers.extend([('Content-Type','application/json'),('Content-Length',str(len(body)))])
        start_response(status,headers)
        return [] if method=='HEAD' else [body]
    return app


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true',help='Run synchronously instead of serving HTTP')
    args=parser.parse_args();research=Research()
    if args.run:
        research.run();print(json.dumps(research.status(),indent=2))
        raise SystemExit(0 if research.state['status']=='COMPLETE' else 1)
    class Quiet(WSGIRequestHandler):
        def log_message(self,*args):pass
    port=int(os.getenv('PORT','8080'))
    print(json.dumps(dict(service=e.VERSION,port=port,orders_supported=False)),flush=True)
    with make_server('0.0.0.0',port,application(research),handler_class=Quiet) as server:
        server.serve_forever()


if __name__=='__main__':main()
