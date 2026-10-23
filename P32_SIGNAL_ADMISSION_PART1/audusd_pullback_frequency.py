#!/usr/bin/env python3
"""AUD/USD setup frequency diagnosis; native historical candles only."""
import argparse
import json
import os
from wsgiref.simple_server import make_server,WSGIRequestHandler
from audusd_matrix.io import canon
from audusd_frequency.runner import Research,VERSION


def application(job):
    def app(environ,start_response):
        method=environ.get('REQUEST_METHOD','GET');path=environ.get('PATH_INFO','/')
        status='200 OK';headers=[('Cache-Control','no-store')]
        if method not in ('GET','HEAD'):
            body=canon(dict(error='GET and HEAD only',orders_supported=False));status='405 Method Not Allowed'
        elif path in ('/','/health'):
            body=canon(dict(service=VERSION,accounts=0,controls=3,orders_supported=False,
                orders_sent=False,start_once='/status',protocol='/protocol',result='/results-summary.zip',
                full_result='/results.zip',live_portfolio_modified=False))
        elif path=='/protocol':body=canon(dict(signature=job.signature,**job.protocol))
        elif path=='/status':body=canon(job.status(start=method=='GET'))
        elif path in ('/results-summary.zip','/results.zip'):
            if job.state['status']!='COMPLETE':
                body=canon(dict(error='Diagnostic not complete; see /status'));status='404 Not Found'
            else:
                summary=path=='/results-summary.zip'
                name='AUDUSD_FREQUENCY_SUMMARY.zip' if summary else 'AUDUSD_FREQUENCY_FULL.zip'
                file=job.work/name
                # Rebuild a missing ZIP from verified completed report files.
                if not file.is_file():job.archive(file,summary)
                start_response(status,headers+[('Content-Type','application/zip'),
                    ('Content-Length',str(file.stat().st_size)),('Content-Disposition','attachment; filename='+name)])
                def chunks():
                    with file.open('rb') as f:
                        for block in iter(lambda:f.read(65536),b''):yield block
                return [] if method=='HEAD' else chunks()
        else:body=canon(dict(error='Unknown route'));status='404 Not Found'
        start_response(status,headers+[('Content-Type','application/json'),('Content-Length',str(len(body)))])
        return [] if method=='HEAD' else [body]
    return app


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true');args=parser.parse_args()
    job=Research()
    if args.run:
        job.run();print(json.dumps(job.status(),indent=2));raise SystemExit(0 if job.state['status']=='COMPLETE' else 1)
    class Quiet(WSGIRequestHandler):
        def log_message(self,*args):pass
    with make_server('0.0.0.0',int(os.getenv('PORT','8080')),application(job),handler_class=Quiet) as server:server.serve_forever()


if __name__=='__main__':main()
