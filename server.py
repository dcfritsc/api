import http.server
import socketserver
import app

PORT = 80
Handler = http.server.SimpleHTTPRequestHandler

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    app.app
    httpd.serve_forever()