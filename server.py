import asyncio
import json
import os
import base64
import queue
import threading
import urllib.request
import urllib.error
import aiohttp
from aiohttp import web

# Google Cloud Speech (Chirp 3) - 遅延インポート（未インストールでも他機能は動く）
try:
    from google.cloud import speech_v2
    from google.cloud.speech_v2.types import cloud_speech
    from google.oauth2 import service_account
    GOOGLE_SPEECH_AVAILABLE = True
except Exception as _e:
    GOOGLE_SPEECH_AVAILABLE = False
    print('google-cloud-speech 未導入:', _e)

# WebSocket接続管理
ws_clients = set()

# サーバー側設定保存（メモリ）
server_settings = {
    'dict': [],
    'context': '',
    'catDict': {},
    'catContext': {},
    'langList': []
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

def check_passphrase(passphrase):
    # 環境変数APP_PASSPHRASEが未設定なら合言葉チェックをスキップ
    required = os.environ.get('APP_PASSPHRASE', '')
    if not required:
        return True
    return passphrase == required

# Google Chirp 3 認証情報の読み込み
_chirp_credentials = None
_chirp_project_id = None

def get_chirp_credentials():
    global _chirp_credentials, _chirp_project_id
    if _chirp_credentials is not None:
        return _chirp_credentials, _chirp_project_id
    raw = os.environ.get('GOOGLE_SA_JSON', '')
    if not raw:
        return None, None
    try:
        info = json.loads(raw)
        _chirp_credentials = service_account.Credentials.from_service_account_info(info)
        _chirp_project_id = info.get('project_id')
        return _chirp_credentials, _chirp_project_id
    except Exception as e:
        print('Chirp認証読み込みエラー:', e)
        return None, None

async def config_handler(request):
    # クライアントに必要な設定状況を返す（キー自体は返さない）
    passphrase = request.query.get('passphrase', '')
    if not check_passphrase(passphrase):
        return web.Response(status=401, body=json.dumps({'error': 'invalid passphrase'}).encode(),
            headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'})
    result = {
        'anthropic': bool(os.environ.get('ANTHROPIC_API_KEY', '')),
        'google': bool(os.environ.get('GOOGLE_STT_API_KEY', '')),
        'assemblyai': bool(os.environ.get('ASSEMBLYAI_API_KEY', '')),
        'chirp': bool(os.environ.get('GOOGLE_SA_JSON', '')) and GOOGLE_SPEECH_AVAILABLE,
        'passphrase_required': bool(os.environ.get('APP_PASSPHRASE', '')),
    }
    return web.Response(body=json.dumps(result).encode(),
        headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'})

async def verify_passphrase_handler(request):
    data = await request.json()
    ok = check_passphrase(data.get('passphrase', ''))
    return web.Response(body=json.dumps({'ok': ok}).encode(),
        headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'})

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
        # 送られてきたキーをそのまま保存（柔軟な箱方式）
        for k, v in data.items():
            server_settings[k] = v
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
    # 合言葉チェック
    passphrase = data.pop('passphrase', '')
    if not check_passphrase(passphrase):
        return web.Response(status=401, body=json.dumps({'error': 'invalid passphrase'}).encode(),
            headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'})
    # サーバー環境変数のキーを優先、なければクライアントのキー
    api_key = os.environ.get('ANTHROPIC_API_KEY', '') or data.pop('api_key', '')
    data.pop('api_key', None)
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
    passphrase = data.pop('passphrase', '')
    if not check_passphrase(passphrase):
        return web.Response(status=401, body=json.dumps({'error': 'invalid passphrase'}).encode(),
            headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'})
    api_key = os.environ.get('GOOGLE_STT_API_KEY', '') or data.pop('api_key', '')
    data.pop('api_key', None)
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

async def assemblyai_token_handler(request):
    passphrase = request.query.get('passphrase', '')
    if not check_passphrase(passphrase):
        return web.Response(status=401, body=json.dumps({'error': 'invalid passphrase'}).encode(),
            headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'})
    api_key = os.environ.get('ASSEMBLYAI_API_KEY', '')
    if not api_key:
        return web.Response(
            status=400,
            body=json.dumps({'error': 'ASSEMBLYAI_API_KEY not set'}).encode(),
            headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        )
    try:
        # v3 API token endpoint (GET with query params)
        # expires_in_seconds: 1-600秒（トークン有効期限）
        # max_session_duration_seconds: 最大10800秒（セッション継続時間）
        req = urllib.request.Request(
            'https://streaming.assemblyai.com/v3/token?expires_in_seconds=600&max_session_duration_seconds=10800',
            headers={
                'Authorization': api_key,
            },
            method='GET'
        )
        with urllib.request.urlopen(req) as res:
            result = json.loads(res.read())
        return web.Response(
            body=json.dumps({'token': result['token']}).encode(),
            headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        )
    except Exception as e:
        print(f'AssemblyAI token error: {e}')
        return web.Response(
            status=500,
            body=json.dumps({'error': str(e)}).encode(),
            headers={'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        )


async def chirp_ws_handler(request):
    """講師ブラウザからのPCM音声をChirp 3ストリーミングに中継"""
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    passphrase = request.query.get('passphrase', '')
    if not check_passphrase(passphrase):
        await ws.send_str(json.dumps({'type': 'error', 'message': 'invalid passphrase'}))
        await ws.close()
        return ws

    if not GOOGLE_SPEECH_AVAILABLE:
        await ws.send_str(json.dumps({'type': 'error', 'message': 'google-cloud-speech not installed'}))
        await ws.close()
        return ws

    credentials, project_id = get_chirp_credentials()
    if not credentials:
        await ws.send_str(json.dumps({'type': 'error', 'message': 'GOOGLE_SA_JSON not set'}))
        await ws.close()
        return ws

    sample_rate = int(request.query.get('sample_rate', '16000'))
    print(f'[Chirp] 受信サンプルレート: {sample_rate}', flush=True)
    loop = asyncio.get_event_loop()
    audio_q = queue.Queue()
    stop_flag = threading.Event()

    def request_generator():
        recognition_config = cloud_speech.RecognitionConfig(
            explicit_decoding_config=cloud_speech.ExplicitDecodingConfig(
                encoding=cloud_speech.ExplicitDecodingConfig.AudioEncoding.LINEAR16,
                sample_rate_hertz=sample_rate,
                audio_channel_count=1,
            ),
            language_codes=['ja-JP'],
            model='chirp_3',
        )
        streaming_config = cloud_speech.StreamingRecognitionConfig(
            config=recognition_config,
            streaming_features=cloud_speech.StreamingRecognitionFeatures(
                interim_results=True,
            ),
        )
        # 1) 設定リクエスト
        yield cloud_speech.StreamingRecognizeRequest(
            recognizer=f'projects/{project_id}/locations/us/recognizers/_',
            streaming_config=streaming_config,
        )
        print('[Chirp] config送信', flush=True)
        # 2) 音声チャンクを流し続ける
        sent = 0
        while not stop_flag.is_set():
            try:
                chunk = audio_q.get(timeout=0.1)
            except queue.Empty:
                continue
            if chunk is None:
                break
            sent += 1
            if sent % 40 == 1:
                print(f'[Chirp] 音声送信 {sent}チャンク目', flush=True)
            yield cloud_speech.StreamingRecognizeRequest(audio=chunk)

    def run_stream():
        try:
            from google.api_core import client_options as client_options_lib
            region = 'us'
            client = speech_v2.SpeechClient(
                credentials=credentials,
                client_options=client_options_lib.ClientOptions(
                    api_endpoint=f'{region}-speech.googleapis.com'
                )
            )
            print('[Chirp] Googleへstreaming開始', flush=True)
            responses = client.streaming_recognize(requests=request_generator())
            print('[Chirp] Google応答ループ入り', flush=True)
            for response in responses:
                for result in response.results:
                    if not result.alternatives:
                        continue
                    transcript = result.alternatives[0].transcript
                    is_final = result.is_final
                    print(f'[Chirp] 応答: final={is_final} "{transcript[:20]}"', flush=True)
                    msg = json.dumps({
                        'type': 'transcript',
                        'transcript': transcript,
                        'is_final': is_final,
                    })
                    try:
                        asyncio.run_coroutine_threadsafe(ws.send_str(msg), loop)
                    except Exception as se:
                        print('[Chirp] 送信失敗:', se, flush=True)
        except Exception as e:
            err = json.dumps({'type': 'error', 'message': str(e)})
            try:
                asyncio.run_coroutine_threadsafe(ws.send_str(err), loop)
            except:
                pass
            print('[Chirp] streamエラー:', repr(e), flush=True)

    stream_thread = threading.Thread(target=run_stream, daemon=True)
    stream_thread.start()
    print('Chirp接続開始', flush=True)

    recv_count = 0
    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                recv_count += 1
                if recv_count % 40 == 1:
                    print(f'[Chirp] ブラウザから音声受信 {recv_count}個目', flush=True)
                audio_q.put(msg.data)
            elif msg.type == aiohttp.WSMsgType.TEXT:
                if msg.data == 'STOP':
                    break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                break
    finally:
        stop_flag.set()
        audio_q.put(None)
        print('Chirp接続終了', flush=True)
    return ws

app.router.add_get('/ws', ws_handler)
app.router.add_get('/chirp-ws', chirp_ws_handler)
app.router.add_get('/settings', get_settings_handler)
app.router.add_post('/settings', post_settings_handler)
app.router.add_post('/api', api_handler)
app.router.add_post('/speech', speech_handler)
app.router.add_get('/assemblyai-token', assemblyai_token_handler)
app.router.add_get('/config', config_handler)
app.router.add_post('/verify-passphrase', verify_passphrase_handler)
app.router.add_route('OPTIONS', '/{path_info:.*}', options_handler)
app.router.add_get('/', lambda r: web.HTTPFound('/instructor.html'))
app.router.add_get('/{filename}', static_handler)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    print(f'サーバー起動中... ポート{port}')
    web.run_app(app, host='0.0.0.0', port=port)
