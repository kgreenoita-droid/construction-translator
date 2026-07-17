import asyncio
import json
import os
import urllib.request
import urllib.error
import aiohttp
from aiohttp import web

# WebSocket接続管理
ws_clients = set()

# サーバー側設定保存（メモリ）
server_settings = {
    'dict': [],
    'context': ''
}

# 設定ファイルのパス
SETTINGS_FILE = 'settings.json'

def load_settings():
    global server_settings
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                server_settings = json.load(f)
            print(f'設定読み込み完了: 辞書{len(server_settings.get("dict",[]))}件')
    except Exception as e:
        print(f'設定読み込みエラー: {e}')

def save_settings():
    try:
        with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(server_settings, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f'設定保存エラー: {e}')

async def ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    ws_clients.add(ws)
    print(f'WS接続 合計{len(ws_clients)}台')
    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                for client in list(ws_clients):
                    if client is not ws:
                        try:
                            await client.send_str(msg.data)
                        except:
                            pass
            elif msg.type == aiohttp.WSMsgType.ERROR:
                break
    finally:
        ws_clients.discard(ws)
        print(f'WS切断 残り{len(ws_clients)}台')
    return ws

async def get_settings_handler(request):
    return web.Response(
        body=json.dumps(server_settings, ensure_ascii=False).encode('utf-8'),
        headers={
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    )

async def post_settings_handler(request):
    global server_settings
    try:
        data = await request.json()
        if 'dict' in data:
            server_settings['dict'] = data['dict']
        if 'context' in data:
            server_settings['context'] = data['context']
        save_settings()
        return web.Response(
            body=json.dumps({'status': 'ok'}).encode(),
            headers={
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        )
    except Exception as e:
        return web.Response(
            status=500,
            body=json.dumps({'error': str(e)}).encode(),
            headers={
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        )

async def api_handler(request):
    data = await request.json()
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
        response = web.StreamResponse()
        response.headers['Content-Type'] = 'text/event-stream'
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['Access-Control-Allow-Origin'] = '*'
        await response.prepare(request)
        try:
            with urllib.request.urlopen(req) as res:
                while True:
                    chunk = res.read(1024)
                    if not chunk:
                        break
                    await response.write(chunk)
        except Exception as e:
            print('Stream error:', e)
        return response
    else:
        with urllib.request.urlopen(req) as res:
            result = res.read()
        return web.Response(
            body=result,
            headers={
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        )

async def speech_handler(request):
    data = await request.json()
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
        return web.Response(
            body=result,
            headers={
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        )
    except urllib.error.HTTPError as e:
        body = e.read()
        return web.Response(
            status=e.code,
            body=body,
            headers={
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        )

async def options_handler(request):
    return web.Response(
        headers={
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': '*',
        }
    )

async def static_handler(request):
    filename = request.match_info.get('filename', 'instructor.html')
    if not filename:
        filename = 'instructor.html'
    if '..' in filename or filename.startswith('/'):
        raise web.HTTPForbidden()
    ext = filename.split('.')[-1].lower()
    content_types = {
        'html': 'text/html; charset=utf-8',
        'js': 'application/javascript',
        'css': 'text/css',
        'json': 'application/json',
    }
    ct = content_types.get(ext, 'application/octet-stream')
    try:
        with open(filename, 'rb') as f:
            content = f.read()
        return web.Response(
            body=content,
            headers={
                'Content-Type': ct,
                'Access-Control-Allow-Origin': '*'
            }
        )
    except FileNotFoundError:
        raise web.HTTPNotFound()

# 起動時に設定を読み込み
load_settings()

app = web.Application()
app.router.add_get('/ws', ws_handler)
app.router.add_get('/settings', get_settings_handler)
app.router.add_post('/settings', post_settings_handler)
app.router.add_post('/api', api_handler)
app.router.add_post('/speech', speech_handler)
app.router.add_route('OPTIONS', '/{path_info:.*}', options_handler)
app.router.add_get('/', lambda r: web.HTTPFound('/instructor.html'))
app.router.add_get('/{filename}', static_handler)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print(f'サーバー起動中... ポート{port}')
    web.run_app(app, host='0.0.0.0', port=port)
