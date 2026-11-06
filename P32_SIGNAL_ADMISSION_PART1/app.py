"""Separate research service: stdlib HTTP server, no live-service imports."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
from pathlib import Path
from datetime import datetime,timezone
import json,os
from replay import AuditRun,VERSION,dumps

class Handler(BaseHTTPRequestHandler):
    run=None
    def send_json(self,body,status=200):
        data=dumps(body).encode()
        self.send_response(status)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(data)))
        self.send_header('Cache-Control','no-store')
        self.end_headers();self.wfile.write(data)

    def do_GET(self):
        path=urlsplit(self.path).path
        if path=='/':
            self.send_json(dict(service=VERSION,scope='SEPARATE_READ_ONLY_QUALIFICATION_REPLAY',
                orders_supported=False,orders_sent=False,start_once='/status',
                result='/results-summary.zip',start_utc=self.run.state['start_utc'],
                end_utc=self.run.state['end_utc'],strategy_count=32,
                persistence=self.run.state['persistence']))
        elif path=='/status':
            try:self.run.start_once()
            except RuntimeError as e:self.send_json(dict(status='BUSY',error=str(e),orders_sent=False),409);return
            with self.run.lock:state=dict(self.run.state)
            state['worker_alive']=bool(self.run.thread and self.run.thread.is_alive())
            self.send_json(state)
        elif path in {'/results.zip','/results-summary.zip'}:
            if not self.run.result_path.exists():self.send_json(dict(status='NOT_READY',orders_sent=False),409);return
            size=self.run.result_path.stat().st_size
            self.send_response(200);self.send_header('Content-Type','application/zip')
            self.send_header('Content-Disposition','attachment; filename="P32_RECENT_LIVE_REPLAY_SUMMARY.zip"')
            self.send_header('Content-Length',str(size));self.end_headers()
            with self.run.result_path.open('rb') as source:
                while True:
                    block=source.read(65536)
                    if not block:break
                    self.wfile.write(block)
        else:self.send_json(dict(status='NOT_FOUND',orders_sent=False),404)

    def do_POST(self):self.send_json(dict(status='METHOD_NOT_ALLOWED',orders_supported=False),405)
    def log_message(self,*args):pass

if __name__=='__main__':
    Handler.run=AuditRun()
    ThreadingHTTPServer(('0.0.0.0',int(os.getenv('PORT','8080'))),Handler).serve_forever()
