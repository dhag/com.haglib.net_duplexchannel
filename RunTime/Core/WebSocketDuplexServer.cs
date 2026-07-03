using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// WebSocket双方向通信サーバー
    /// ブラウザから直接接続可能
    /// </summary>
    public class WebSocketDuplexServer : IDuplexServer
    {
        private HttpListener _listener;
        private readonly ConcurrentDictionary<string, WebSocketDuplexChannel> _clients;
        private readonly CancellationTokenSource _cts;
        private Task _acceptTask;
        private bool _disposed;
        private int _clientIdCounter;

        public bool IsListening => _listener?.IsListening ?? false;

        /// <summary>
        /// 非WebSocketのHTTP GET要求に対して返すHTMLを供給する。
        /// null の場合は既定の簡易HTMLを返す。
        /// </summary>
        public Func<string> IndexHtmlProvider { get; set; }

        /// <summary>kind省略時に使用する既定の送信フレーム種別（クライアント受理時にChannelへ伝播）</summary>
        public WebSocketFrameKind DefaultFrame { get; set; } = WebSocketFrameKind.Text;

        public IDuplexChannel[] Clients => _clients.Values.ToArray<IDuplexChannel>();

        public event Action<IDuplexChannel> OnClientConnected;
        public event Action<IDuplexChannel> OnClientDisconnected;
        public event Action<IDuplexChannel, DuplexMessage> OnReceived;

        public WebSocketDuplexServer()
        {
            _clients = new ConcurrentDictionary<string, WebSocketDuplexChannel>();
            _cts = new CancellationTokenSource();
        }

        /// <summary>
        /// リッスン開始（IDuplexServer実装）
        /// WebSocketの場合、ポート番号のみ指定でlocalhostにリッスン
        /// </summary>
        public Task StartAsync(int port, CancellationToken ct = default)
        {
            return StartAsync($"http://localhost:{port}/", ct);
        }

        /// <summary>
        /// リッスン開始（パス指定）
        /// </summary>
        /// <param name="port">ポート番号</param>
        /// <param name="path">パス (例: "/ws")</param>
        /// <param name="useLocalhost">trueの場合localhost、falseの場合+（全インターフェース、要管理者権限）</param>
        public Task StartAsync(int port, string path, bool useLocalhost = true, CancellationToken ct = default)
        {
            var host = useLocalhost ? "localhost" : "+";
            return StartAsync($"http://{host}:{port}{path}", ct);
        }

        /// <summary>
        /// リッスン開始（プレフィックス指定）
        /// </summary>
        public Task StartAsync(string prefix, CancellationToken ct = default)
        {
            if (_listener != null)
                throw new InvalidOperationException("Server is already running.");

            _listener = new HttpListener();
            _listener.Prefixes.Add(prefix.EndsWith("/") ? prefix : prefix + "/");
            _listener.Start();

            _acceptTask = Task.Run(() => AcceptLoopAsync(_cts.Token), ct);

            return Task.CompletedTask;
        }

        /// <summary>
        /// 全クライアントにブロードキャスト
        /// </summary>
        public async Task BroadcastAsync(DuplexMessage message, CancellationToken ct = default)
        {
            var tasks = new List<Task>(_clients.Count);
            foreach (var client in _clients.Values)
            {
                tasks.Add(client.SendAsync(message, ct));
            }

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch { }
        }

        public Task BroadcastAsync(string text, CancellationToken ct = default)
            => BroadcastAsync(new DuplexMessage(text), ct);

        public Task BroadcastAsync(TypedPayload payload, CancellationToken ct = default)
            => BroadcastAsync(payload.ToMessage(), ct);

        /// <summary>
        /// 全クライアントにブロードキャスト（フレーム種別指定）
        /// </summary>
        public async Task BroadcastAsync(DuplexMessage message, WebSocketFrameKind kind, CancellationToken ct = default)
        {
            var tasks = new List<Task>(_clients.Count);
            foreach (var client in _clients.Values)
            {
                tasks.Add(client.SendAsync(message, kind, ct));
            }

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch { }
        }

        /// <summary>
        /// 特定のクライアント以外に送信
        /// </summary>
        public async Task BroadcastExceptAsync(string excludeClientId, DuplexMessage message, CancellationToken ct = default)
        {
            foreach (var client in _clients.Values)
            {
                if (client.Id != excludeClientId)
                {
                    try
                    {
                        await client.SendAsync(message, ct).ConfigureAwait(false);
                    }
                    catch { }
                }
            }
        }

        /// <summary>
        /// 特定のクライアント以外に送信（フレーム種別指定）
        /// </summary>
        public async Task BroadcastExceptAsync(string excludeClientId, DuplexMessage message, WebSocketFrameKind kind, CancellationToken ct = default)
        {
            foreach (var client in _clients.Values)
            {
                if (client.Id != excludeClientId)
                {
                    try
                    {
                        await client.SendAsync(message, kind, ct).ConfigureAwait(false);
                    }
                    catch { }
                }
            }
        }

        /// <summary>
        /// サーバー停止
        /// </summary>
        public async Task StopAsync()
        {
            if (_disposed) return;

            _cts.Cancel();

            try
            {
                _listener?.Stop();
            }
            catch { }

            foreach (var client in _clients.Values)
            {
                try
                {
                    await client.CloseAsync().ConfigureAwait(false);
                }
                catch { }
            }
            _clients.Clear();

            if (_acceptTask != null)
            {
                try
                {
                    await _acceptTask.ConfigureAwait(false);
                }
                catch { }
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _cts.Cancel();

            try { _listener?.Stop(); } catch { }

            foreach (var client in _clients.Values)
            {
                client.Dispose();
            }
            _clients.Clear();

            _cts.Dispose();
        }

        private async Task AcceptLoopAsync(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var context = await _listener.GetContextAsync().ConfigureAwait(false);

                    if (!context.Request.IsWebSocketRequest)
                    {
                        // CORSヘッダーを追加してHTMLを返す（デバッグ用）
                        context.Response.StatusCode = 200;
                        context.Response.ContentType = "text/html";
                        context.Response.AddHeader("Access-Control-Allow-Origin", "*");
                        var html = Encoding.UTF8.GetBytes(
                            IndexHtmlProvider?.Invoke()
                            ?? "<html><body><h1>WebSocket Server</h1><p>Use WebSocket to connect.</p></body></html>");
                        await context.Response.OutputStream.WriteAsync(html, 0, html.Length, ct).ConfigureAwait(false);
                        context.Response.Close();
                        continue;
                    }

                    try
                    {
                        var wsContext = await context.AcceptWebSocketAsync(null).ConfigureAwait(false);
                        var clientId = $"W{Interlocked.Increment(ref _clientIdCounter):D4}";

                        var channel = new WebSocketDuplexChannel(wsContext.WebSocket, clientId);
                        channel.DefaultFrame = DefaultFrame;
                        channel.OnReceived += (ch, msg) => OnReceived?.Invoke(ch, msg);
                        channel.OnDisconnected += HandleClientDisconnected;

                        _clients[clientId] = channel;
                        channel.StartReceiving(ct);

                        OnClientConnected?.Invoke(channel);
                    }
                    catch (Exception ex)
                    {
                        System.Diagnostics.Debug.WriteLine($"WebSocket accept error: {ex.Message}");
                    }
                }
            }
            catch (HttpListenerException) { }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
        }

        private void HandleClientDisconnected(IDuplexChannel channel)
        {
            if (_clients.TryRemove(channel.Id, out _))
            {
                OnClientDisconnected?.Invoke(channel);
            }
        }
    }
}
