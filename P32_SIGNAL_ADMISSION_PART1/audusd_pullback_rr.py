#!/usr/bin/env python3
"""Fixed nine-setting AUD/USD extension; historical candle GETs only."""
import argparse
import json
import os
from wsgiref.simple_server import make_server,WSGIRequestHandler
from audusd_matrix.io import canon
from audusd_rr.runner import Research
from audusd_rr.config import VERSION
from audusd_rr.download import Stream


def application(job):
    def app(environ,start_response):
        method=environ.get('REQUEST_METHOD','GET');path=environ.get('PATH_INFO','/')
        status='200 OK';headers=[('Cache-Control','no-store')]
        if method not in ('GET','HEAD'):status='405 Method Not Allowed';payload=dict(error='GET and HEAD only',orders_supported=False)
        elif path in ('/','/health'):
            payload=dict(service=VERSION,candidates=18,accounts=252,overlap_controls=6,overlap_account_payloads=84,
                         start_once='/status',protocol='/protocol',result='/results-summary.zip',orders_supported=False,orders_sent=False)
        elif path=='/protocol':payload=dict(signature=job.signature,**job.protocol)
        elif path=='/status':payload=job.status(start=method=='GET')
        elif path in ('/results-summary.zip','/results.zip'):
            if not (job.work/'run_state.json').is_file():status='404 Not Found';payload=dict(error='No published candidate yet',status='/status')
            else:
                summary=path=='/results-summary.zip';stream=Stream(job,summary)
                name='AUDUSD_RR_SUMMARY.zip' if summary else 'AUDUSD_RR_FULL_RESULTS.zip'
                start_response(status,headers+[('Content-Type','application/zip'),('Content-Disposition','attachment; filename='+name)])
                return [] if method=='HEAD' else stream
        else:status='404 Not Found';payload=dict(error='Unknown route')
        body=canon(payload);start_response(status,headers+[('Content-Type','application/json'),('Content-Length',str(len(body)))])
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
