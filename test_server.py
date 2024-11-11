from http.server import HTTPServer, BaseHTTPRequestHandler


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        print("Received webhook:", post_data.decode('utf-8'))

        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(b"OK")


server = HTTPServer(('localhost', 8080), WebhookHandler)
print("Server running on port 8080")
server.serve_forever()
