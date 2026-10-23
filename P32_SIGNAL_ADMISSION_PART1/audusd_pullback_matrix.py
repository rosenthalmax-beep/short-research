#!/usr/bin/env python3
"""Focused AUD/USD long conditional matrix; historical candle GETs only."""
import argparse
import json
import os
from wsgiref.simple_server import make_server,WSGIRequestHandler
from audusd_matrix.runner import Research,VERSION
from audusd_matrix.download import Stream
from audusd_matrix.io import canon


def application(job):
    def app(environ,start_response):
        method=environ.get('REQUEST_METHOD','GET');path=environ.get('PATH_INFO','/');status='200 OK';headers=[('Cache-Control','no-store')]
        if method not in ('GET','HEAD'):
            body=canon(dict(error='GET and HEAD only',orders_supported=False));status='405 Method Not Allowed'
        elif path in ('/','/health'):
            body=canon(dict(service=VERSION,candidates=16,matrix_cells=9,accounts=224,orders_supported=False,orders_sent=False,
                start_once='/status',protocol='/protocol',result='/results-summary.zip',full_result='/results.zip',
                live_portfolio_modified=False,read_only_broker_candles=True))
        elif path=='/protocol':body=canon(dict(signature=job.signature,**job.protocol))
        elif path=='/status':body=canon(job.status(start=method=='GET'))
        elif path in ('/results-summary.zip','/results.zip'):
            if not (job.work/'run_state.json').is_file():
                body=canon(dict(error='No published candidate report yet',status='/status'));status='404 Not Found'
            else:
                summary=path=='/results-summary.zip';stream=Stream(job,summary)
                name='AUDUSD_MATRIX_SUMMARY.zip' if summary else 'AUDUSD_MATRIX_FULL_RESULTS.zip'
                start_response('200 OK',headers+[('Content-Type','application/zip'),('Content-Disposition','attachment; filename='+name)])
                return [] if method=='HEAD' else stream
        else:body=canon(dict(error='Unknown route'));status='404 Not Found'
        start_response(status,headers+[('Content-Type','application/json'),('Content-Length',str(len(body)))])
        return [] if method=='HEAD' else [body]
    return app


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true');args=parser.parse_args();job=Research()
    if args.run:
        job.run();print(json.dumps(job.status(),indent=2));raise SystemExit(0 if job.state['status']=='COMPLETE' else 1)
    class Quiet(WSGIRequestHandler):
        def log_message(self,*args):pass
    port=int(os.getenv('PORT','8080'));print(json.dumps(dict(service=VERSION,port=port,orders_supported=False)),flush=True)
    with make_server('0.0.0.0',port,application(job),handler_class=Quiet) as server:server.serve_forever()


if __name__=='__main__':main()
