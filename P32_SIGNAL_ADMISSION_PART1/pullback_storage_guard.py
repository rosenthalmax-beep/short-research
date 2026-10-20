#!/usr/bin/env python3
"""Start the unchanged V2 research service with storage-bounded recovery."""
import argparse
import json
import os
from pathlib import Path
from wsgiref.simple_server import make_server,WSGIRequestHandler
from pullback_app import application as original_application
from pullback_lab.runner import Research,persistence
from pullback_guard.runtime import configure,VERSION
from pullback_guard.download import Stream


def application(job):
    original=original_application(job)
    def app(environ,start_response):
        path=environ.get('PATH_INFO','/');method=environ.get('REQUEST_METHOD','GET')
        if method in ('GET','HEAD') and path in ('/','/health'):
            body=json.dumps(dict(service=VERSION,calculation_version=job.protocol['version'],
                signature=job.signature,orders_supported=False,orders_sent=False,start_once='/status',
                result='/results.zip',summary='/results-summary.zip',protocol='/protocol',
                persistence=persistence(job.directory),full_results_streamed=True)).encode()
            start_response('200 OK',[('Content-Type','application/json'),('Cache-Control','no-store'),('Content-Length',str(len(body)))])
            return [] if method=='HEAD' else [body]
        if method in ('GET','HEAD') and path in ('/results.zip','/results-summary.zip'):
            ready=any((job.work/'pairs'/p/'COMPLETE.json').is_file() for p in job.protocol['pairs'])
            if not ready:
                body=b'{"error":"No completed pair report yet","status":"/status"}'
                start_response('404 Not Found',[('Content-Type','application/json'),('Content-Length',str(len(body)))])
                return [] if method=='HEAD' else [body]
            summary=path=='/results-summary.zip'
            stream=Stream(job,summary)
            filename='P32_PULLBACK_SUMMARY.zip' if summary else 'P32_PULLBACK_PASS1_RESULTS.zip'
            start_response('200 OK',[('Content-Type','application/zip'),('Cache-Control','no-store'),
                                   ('Content-Disposition','attachment; filename='+filename)])
            return [] if method=='HEAD' else stream
        return original(environ,start_response)
    return app


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true');args=parser.parse_args()
    job=configure(Research(),Path(__file__).resolve().parent)
    if args.run:
        job.run();print(json.dumps(job.status(),indent=2));raise SystemExit(0 if job.state['status']=='COMPLETE' else 1)
    class Quiet(WSGIRequestHandler):
        def log_message(self,*args):pass
    port=int(os.getenv('PORT','8080'))
    print(json.dumps(dict(service=VERSION,port=port,orders_supported=False)),flush=True)
    with make_server('0.0.0.0',port,application(job),handler_class=Quiet) as server:server.serve_forever()


if __name__=='__main__':main()
