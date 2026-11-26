"""
Stub HTTP data center for test/retry/retry_test.pl.  Prints the port it listens on, then
serves until killed.  The first part of the path chooses the behaviour:

  /data/ok/F            200, content "content of /data/ok/F"
  /data/missing/F       404
  /data/forbidden/F     403
  /data/throttle1/F     429 with Retry-After: 1 the first time, then 200
  /data/unavailable2/F  503 (no Retry-After) the first two times, then 200
  /data/always503/F     503 every time
  /data/longwait/F      429 with Retry-After: 1000
  /data/slowonce/F      waits 3 seconds the first time, then 200
  /list/D               503 the first time, then a list of file names
  /auth                 429 with Retry-After: 1 the first time, then the GNSS Archive
                        auth response with a cookie
  /archive/F            200 if the auth cookie is sent, otherwise 403
  /archive503/F         503 the first time, then as /archive/F
  /_stats               JSON: for each path the number of requests, the client ports
                        used, and whether an Authorization header was sent
"""

import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

lock = threading.Lock()
stats = {}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass

    def send(self, code, body=b"", headers=None):
        self.send_response(code)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path
        if path == "/_stats":
            with lock:
                body = json.dumps(stats).encode()
            return self.send(200, body)
        with lock:
            entry = stats.setdefault(path, {"count": 0, "ports": [], "authorization": False})
            entry["count"] += 1
            entry["ports"].append(self.client_address[1])
            if self.headers.get("Authorization"):
                entry["authorization"] = True
            count = entry["count"]
        kind = path.split("/")[2] if path.startswith("/data/") else path.split("/")[1]
        content = ("content of %s" % path).encode()
        if kind == "ok":
            return self.send(200, content)
        if kind == "missing":
            return self.send(404)
        if kind == "forbidden":
            return self.send(403)
        if kind == "throttle1":
            if count == 1:
                return self.send(429, headers={"Retry-After": "1"})
            return self.send(200, content)
        if kind == "unavailable2":
            if count <= 2:
                return self.send(503)
            return self.send(200, content)
        if kind == "always503":
            return self.send(503)
        if kind == "longwait":
            return self.send(429, headers={"Retry-After": "1000"})
        if kind == "slowonce":
            if count == 1:
                time.sleep(3)
            return self.send(200, content)
        if kind == "list":
            if count == 1:
                return self.send(503)
            return self.send(200, b"f1\nf2\nf3\n")
        if kind == "auth":
            if count == 1:
                return self.send(429, headers={"Retry-After": "1"})
            body = json.dumps({"cookies": ["archiveauth=ok; Path=/; HttpOnly"]}).encode()
            return self.send(200, body)
        if kind == "archive503" and count == 1:
            return self.send(503)
        if kind in ("archive", "archive503"):
            if "archiveauth=ok" in (self.headers.get("Cookie") or ""):
                return self.send(200, content)
            return self.send(403)
        return self.send(404)


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
print(server.server_address[1], flush=True)
server.serve_forever()
