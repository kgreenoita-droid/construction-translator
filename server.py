import asyncio
import json
import os
import urllib.request
import urllib.error
import websockets
from websockets.server import serve
from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer
import threading

# WebSocket接続管理
ws_clients = set()
ws_lock = asyncio.Lock()

async def ws_handler(websocket):
    async with ws_lock:
        ws_clients.add(websocket)
    print(f'WS接続 合計{len(ws_clients)}台')
    try:
        async for message in websocket:
            async with ws_lock:
                targets = [c for c in ws_clients if c is not websocket]
            if targets:
                await asyncio.gather(
                    *[c.send(message) for c in targets],
                    return_exceptions=True
                )
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        async with ws_lock:
            ws_clients.discard(websocket)
        print(f'WS切断 残り{len(ws_clients)}台')

async def http_handler(reader, writer):
    """簡易HTTPサーバー（静的ファイル配信＋APIプロキシ）"""
    try:
        request = await reader.read(8192)
        if not request:
            return

        lines = request.decode('utf-8', errors='replace').split('\r\n')
        if not lines:
            return

        first_line = lines[0].split(' ')
        if len(first_line) < 2:
            return

        method = first_line[0]
        path = first_line[1]

        # ヘッダー解析
        headers = {}
        for line in lines[1:]:
            if ': ' in line:
                k, v = line.split(': ', 1)
                headers[k.lower()] = v

        # CORSヘッダー
        cors = (
            'Access-Control-Allow-Origin: *\r\n'
            'Access-Control-Allow-Headers: *\r\n'
            'Access-Control-Allow-Methods: *\r\n'
        )

        if method == 'OPTIONS':
            writer.write(f'HTTP/1.1 200 OK\r\n{cors}\r\n'.encode())
            await writer.drain()
            return

        if method == 'POST':
            content_length = int(headers.get('content-length', 0))
            body_start = request.find(b'\r\n\r\n') + 4
            body = request[body_start:]
            while len(body) < content_length:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                body += chunk

            data = json.loads(body)

            if path == '/api':
                api_key = data.pop('api_key')
                is_stream = data.get('stream', False)
                req = urllib.request.Request(
                    'https://api.anthropic.com/v1/messages',
                    data=json.dumps(data).encode(),
                    headers={
                        'Content-Type': 'application/json',
                        'x-api-key': api_key,
                        'anthropic-version': '2023-06-01',
                    },
                    method='POST'
                )
                if is_stream:
                    try:
                        with urllib.request.urlopen(req) as res:
                            writer.write(
                                f'HTTP/1.1 200 OK\r\n{cors}'
                                'Content-Type: text/event-stream\r\n'
                                'Cache-Control: no-cache\r\n\r\n'.encode()
                            )
                            await writer.drain()
                            while True:
                                chunk = res.read(1024)
                                if not chunk:
                                    break
                                writer.write(chunk)
                                await writer.drain()
                    except Exception as e:
                        print('Stream error:', e)
                else:
                    with urllib.request.urlopen(req) as res:
                        result = res.read()
                    writer.write(
                        f'HTTP/1.1 200 OK\r\n{cors}'
                        'Content-Type: application/json\r\n\r\n'.encode()
                        + result
                    )
                    await writer.drain()

            elif path == '/speech':
                api_key = data.pop('api_key')
                payload = {
                    'config': {
                        'encoding': 'WEBM_OPUS',
                        'sampleRateHertz': 48000,
                        'languageCode': data.get('lang', 'ja-JP'),
                        'model': 'latest_long',
                        'useEnhanced': True,
                        'enableAutomaticPunctuation': True,
                    },
                    'audio': {'content': data.get('audio')}
                }
                url = 'https://speech.googleapis.com/v1/speech:recognize?key=' + api_key
                req = urllib.request.Request(
                    url,
                    data=json.dumps(payload).encode(),
                    headers={'Content-Type': 'application/json'},
                    method='POST'
                )
                try:
                    with urllib.request.urlopen(req) as res:
                        result = res.read()
                    writer.write(
                        f'HTTP/1.1 200 OK\r\n{cors}'
                        'Content-Type: application/json\r\n\r\n'.encode()
                        + result
                    )
                    await writer.drain()
                except urllib.error.HTTPError as e:
                    body = e.read()
                    writer.write(
                        f'HTTP/1.1 {e.code} Error\r\n{cors}'
                        'Content-Type: application/json\r\n\r\n'.encode()
                        + body
                    )
                    await writer.drain()
            return

        # GET: 静的ファイル配信
        if path == '/' or path == '':
            path = '/instructor.html'

        file_path = '.' + path
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
            ext = path.split('.')[-1].lower()
            content_types = {
                'html': 'text/html; charset=utf-8',
                'js': 'application/javascript',
                'css': 'text/css',
                'json': 'application/json',
                'py': 'text/plain',
            }
            ct = content_types.get(ext, 'application/octet-stream')
            writer.write(
                f'HTTP/1.1 200 OK\r\n{cors}'
                f'Content-Type: {ct}\r\n'
                f'Content-Length: {len(content)}\r\n\r\n'.encode()
                + content
            )
        except FileNotFoundError:
            writer.write(
                f'HTTP/1.1 404 Not Found\r\n{cors}\r\n'
                b'Not Found'
            )
        await writer.drain()

    except Exception as e:
        print(f'HTTP error: {e}')
    finally:
        writer.close()

async def main():
    port = int(os.environ.get('PORT', 8080))
    ws_port = int(os.environ.get('WS_PORT', 8081))

    # HTTPサーバー
    http_server = await asyncio.start_server(http_handler, '0.0.0.0', port)
    print(f'HTTPサーバー起動中... ポート{port}')

    # WebSocketサーバー
    ws_server = await serve(ws_handler, '0.0.0.0', ws_port)
    print(f'WebSocketサーバー起動中... ポート{ws_port}')

    async with http_server, ws_server:
        await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())
