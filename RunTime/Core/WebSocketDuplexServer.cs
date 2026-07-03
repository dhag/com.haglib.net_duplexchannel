using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;              // com.unity.nuget.newtonsoft-json (UPM, Runtime/Editor両対応)

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// WebSocket双方向通信サーバー（Unity Mono / IL2CPP 専用）。
    /// HttpListener.AcceptWebSocketAsync は Unity ランタイムで NotImplementedException になるため、
    /// TcpListener 上で RFC6455 のハンドシェイク・フレーミングを自前実装する。
    /// ワイヤ形式（Text=JSON items / Binary=DuplexPacket）は WebSocketDuplexChannel と同一。
    /// </summary>
    public class WebSocketDuplexServer : IDuplexServer
    {
        private TcpListener _listenerV4;
        private TcpListener _listenerV6;
        private readonly ConcurrentDictionary<string, TcpDuplexServerChannel> _clients;
        private readonly CancellationTokenSource _cts;
        private Task _acceptTaskV4;
        private Task _acceptTaskV6;
        private bool _disposed;
        private bool _listening;
        private int _clientIdCounter;

        public bool IsListening => _listening;

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
            _clients = new ConcurrentDictionary<string, TcpDuplexServerChannel>();
            _cts = new CancellationTokenSource();
        }

        // ================================================================
        // リッスン開始
        // ================================================================

        public Task StartAsync(int port, CancellationToken ct = default)
        {
            return StartInternal(port, ct);
        }

        /// <param name="path">互換のため受け取るが TcpListener 実装では使用しない。</param>
        public Task StartAsync(int port, string path, bool useLocalhost = true, CancellationToken ct = default)
        {
            return StartInternal(port, ct);
        }

        /// <summary>プレフィックス（例 "http://localhost:8765/"）からポートを取り出して開始する。</summary>
        public Task StartAsync(string prefix, CancellationToken ct = default)
        {
            return StartInternal(ExtractPort(prefix), ct);
        }

        private Task StartInternal(int port, CancellationToken ct)
        {
            if (_listenerV4 != null || _listenerV6 != null)
                throw new InvalidOperationException("Server is already running.");

            // IPv4 loopback (127.0.0.1) と IPv6 loopback (::1) の両方を待ち受ける。
            // ブラウザは ::1、Unity クライアントは 127.0.0.1 で接続してくるため。
            try
            {
                _listenerV4 = new TcpListener(IPAddress.Loopback, port);
                _listenerV4.Start();
                _acceptTaskV4 = Task.Run(() => AcceptLoopAsync(_listenerV4, _cts.Token), ct);
            }
            catch (Exception ex)
            {
                UnityEngine.Debug.LogWarning($"[WebSocketDuplexServer] IPv4 loopback listen 失敗: {ex.Message}");
                _listenerV4 = null;
            }

            try
            {
                _listenerV6 = new TcpListener(IPAddress.IPv6Loopback, port);
                _listenerV6.Start();
                _acceptTaskV6 = Task.Run(() => AcceptLoopAsync(_listenerV6, _cts.Token), ct);
            }
            catch (Exception ex)
            {
                UnityEngine.Debug.LogWarning($"[WebSocketDuplexServer] IPv6 loopback listen 失敗: {ex.Message}");
                _listenerV6 = null;
            }

            _listening = _listenerV4 != null || _listenerV6 != null;
            if (!_listening)
                throw new InvalidOperationException("Failed to start listener on both IPv4 and IPv6 loopback.");

            return Task.CompletedTask;
        }

        private static int ExtractPort(string prefix)
        {
            try { return new Uri(prefix).Port; }
            catch
            {
                var digits = new string(prefix.Where(char.IsDigit).ToArray());
                return int.TryParse(digits, out var p) ? p : 8765;
            }
        }

        // ================================================================
        // 受付ループ
        // ================================================================

        private async Task AcceptLoopAsync(TcpListener listener, CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var tcpClient = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    _ = HandleConnectionAsync(tcpClient, ct);
                }
            }
            catch (ObjectDisposedException) { }
            catch (OperationCanceledException) { }
            catch (SocketException) { }
            catch (Exception ex)
            {
                if (!ct.IsCancellationRequested)
                    UnityEngine.Debug.LogWarning($"[WebSocketDuplexServer] accept error: {ex.Message}");
            }
        }

        private async Task HandleConnectionAsync(TcpClient tcpClient, CancellationToken ct)
        {
            NetworkStream stream = null;
            try
            {
                stream = tcpClient.GetStream();
                string httpRequest = await ReadHttpRequestAsync(stream, ct).ConfigureAwait(false);
                if (string.IsNullOrEmpty(httpRequest)) { tcpClient.Close(); return; }

                bool isWs = httpRequest.IndexOf("Upgrade: websocket", StringComparison.OrdinalIgnoreCase) >= 0;

                if (!isWs)
                {
                    string html = IndexHtmlProvider?.Invoke()
                                  ?? "<html><body><h1>WebSocket Server</h1><p>Use WebSocket to connect.</p></body></html>";
                    byte[] body = Encoding.UTF8.GetBytes(html);
                    string resp =
                        "HTTP/1.1 200 OK\r\n" +
                        "Content-Type: text/html; charset=utf-8\r\n" +
                        "Access-Control-Allow-Origin: *\r\n" +
                        "Content-Length: " + body.Length + "\r\n" +
                        "Connection: close\r\n" +
                        "\r\n";
                    byte[] hdr = Encoding.UTF8.GetBytes(resp);
                    await stream.WriteAsync(hdr, 0, hdr.Length, ct).ConfigureAwait(false);
                    await stream.WriteAsync(body, 0, body.Length, ct).ConfigureAwait(false);
                    tcpClient.Close();
                    return;
                }

                string wsKey = ExtractWebSocketKey(httpRequest);
                if (wsKey == null) { tcpClient.Close(); return; }

                string acceptKey = ComputeAcceptKey(wsKey);
                string handshake =
                    "HTTP/1.1 101 Switching Protocols\r\n" +
                    "Upgrade: websocket\r\n" +
                    "Connection: Upgrade\r\n" +
                    "Sec-WebSocket-Accept: " + acceptKey + "\r\n" +
                    "\r\n";
                byte[] hsBytes = Encoding.UTF8.GetBytes(handshake);
                await stream.WriteAsync(hsBytes, 0, hsBytes.Length, ct).ConfigureAwait(false);

                var clientId = $"W{Interlocked.Increment(ref _clientIdCounter):D4}";
                var channel = new TcpDuplexServerChannel(tcpClient, stream, clientId)
                {
                    DefaultFrame = DefaultFrame,
                };
                channel.OnReceived += (ch, msg) => OnReceived?.Invoke(ch, msg);
                channel.OnDisconnected += HandleClientDisconnected;

                _clients[clientId] = channel;
                OnClientConnected?.Invoke(channel);

                channel.StartReceiving(ct);
            }
            catch (OperationCanceledException) { }
            catch (IOException) { }
            catch (Exception ex)
            {
                if (!ct.IsCancellationRequested)
                    UnityEngine.Debug.LogWarning($"[WebSocketDuplexServer] connection error: {ex.Message}");
                try { tcpClient?.Close(); } catch { }
            }
        }

        private void HandleClientDisconnected(IDuplexChannel channel)
        {
            if (_clients.TryRemove(channel.Id, out _))
            {
                OnClientDisconnected?.Invoke(channel);
            }
        }

        // ================================================================
        // ブロードキャスト
        // ================================================================

        public async Task BroadcastAsync(DuplexMessage message, CancellationToken ct = default)
        {
            var tasks = new List<Task>(_clients.Count);
            foreach (var client in _clients.Values) tasks.Add(client.SendAsync(message, ct));
            try { await Task.WhenAll(tasks).ConfigureAwait(false); } catch { }
        }

        public Task BroadcastAsync(string text, CancellationToken ct = default)
            => BroadcastAsync(new DuplexMessage(text), ct);

        public Task BroadcastAsync(TypedPayload payload, CancellationToken ct = default)
            => BroadcastAsync(payload.ToMessage(), ct);

        public async Task BroadcastAsync(DuplexMessage message, WebSocketFrameKind kind, CancellationToken ct = default)
        {
            var tasks = new List<Task>(_clients.Count);
            foreach (var client in _clients.Values) tasks.Add(client.SendAsync(message, kind, ct));
            try { await Task.WhenAll(tasks).ConfigureAwait(false); } catch { }
        }

        public async Task BroadcastExceptAsync(string excludeClientId, DuplexMessage message, CancellationToken ct = default)
        {
            foreach (var client in _clients.Values)
                if (client.Id != excludeClientId)
                    try { await client.SendAsync(message, ct).ConfigureAwait(false); } catch { }
        }

        public async Task BroadcastExceptAsync(string excludeClientId, DuplexMessage message, WebSocketFrameKind kind, CancellationToken ct = default)
        {
            foreach (var client in _clients.Values)
                if (client.Id != excludeClientId)
                    try { await client.SendAsync(message, kind, ct).ConfigureAwait(false); } catch { }
        }

        // ================================================================
        // 停止
        // ================================================================

        public async Task StopAsync()
        {
            if (_disposed) return;

            _cts.Cancel();
            try { _listenerV4?.Stop(); } catch { }
            try { _listenerV6?.Stop(); } catch { }
            _listenerV4 = null;
            _listenerV6 = null;
            _listening = false;

            foreach (var client in _clients.Values)
                try { await client.CloseAsync().ConfigureAwait(false); } catch { }
            _clients.Clear();

            if (_acceptTaskV4 != null) { try { await _acceptTaskV4.ConfigureAwait(false); } catch { } }
            if (_acceptTaskV6 != null) { try { await _acceptTaskV6.ConfigureAwait(false); } catch { } }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _cts.Cancel();
            try { _listenerV4?.Stop(); } catch { }
            try { _listenerV6?.Stop(); } catch { }
            _listenerV4 = null;
            _listenerV6 = null;
            _listening = false;

            foreach (var client in _clients.Values) client.Dispose();
            _clients.Clear();

            _cts.Dispose();
        }

        // ================================================================
        // HTTP / ハンドシェイク ヘルパー
        // ================================================================

        private static async Task<string> ReadHttpRequestAsync(NetworkStream stream, CancellationToken ct)
        {
            var sb = new StringBuilder();
            byte[] buf = new byte[1];
            int crlfCount = 0;

            while (crlfCount < 4)
            {
                int read = await stream.ReadAsync(buf, 0, 1, ct).ConfigureAwait(false);
                if (read <= 0) return null;

                char c = (char)buf[0];
                sb.Append(c);

                if ((crlfCount % 2 == 0 && c == '\r') || (crlfCount % 2 == 1 && c == '\n'))
                    crlfCount++;
                else
                    crlfCount = (c == '\r') ? 1 : 0;

                if (sb.Length > 8192) return null;
            }
            return sb.ToString();
        }

        private static string ExtractWebSocketKey(string httpRequest)
        {
            foreach (string line in httpRequest.Split('\n'))
            {
                string trimmed = line.Trim();
                if (trimmed.StartsWith("Sec-WebSocket-Key:", StringComparison.OrdinalIgnoreCase))
                    return trimmed.Substring("Sec-WebSocket-Key:".Length).Trim();
            }
            return null;
        }

        private static string ComputeAcceptKey(string wsKey)
        {
            string combined = wsKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
            using (var sha1 = SHA1.Create())
                return Convert.ToBase64String(sha1.ComputeHash(Encoding.UTF8.GetBytes(combined)));
        }
    }

    // ====================================================================
    // TcpListener ベースのサーバ側チャネル（IDuplexChannel 実装）
    // RFC6455 のフレーミングを自前実装。ワイヤ形式は WebSocketDuplexChannel と同一。
    // ====================================================================
    public class TcpDuplexServerChannel : IDuplexChannel
    {
        private readonly TcpClient _tcpClient;
        private readonly NetworkStream _stream;
        private readonly ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>> _pendingRequests;
        private readonly SemaphoreSlim _sendLock;
        private int _nextMessageId;
        private bool _disposed;
        private bool _receiveLoopStarted;

        public bool IsConnected => !_disposed && (_tcpClient?.Connected ?? false);
        public string Id { get; }

        public WebSocketFrameKind DefaultFrame { get; set; } = WebSocketFrameKind.Text;

        public event Action<IDuplexChannel, DuplexMessage> OnReceived;
        public event Action<IDuplexChannel> OnDisconnected;

        public TcpDuplexServerChannel(TcpClient tcpClient, NetworkStream stream, string id)
        {
            _tcpClient = tcpClient ?? throw new ArgumentNullException(nameof(tcpClient));
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            _pendingRequests = new ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>>();
            _sendLock = new SemaphoreSlim(1, 1);
            Id = id ?? Guid.NewGuid().ToString("N").Substring(0, 8);
        }

        public void StartReceiving(CancellationToken ct = default)
        {
            if (_receiveLoopStarted)
                throw new InvalidOperationException("Receive loop already started.");
            _receiveLoopStarted = true;
            _ = ReceiveLoopAsync(ct);
        }

        #region IDuplexChannel 実装

        public Task SendAsync(DuplexMessage message, CancellationToken ct = default)
        {
            message.Type = MessageType.Push;
            message.Id = Interlocked.Increment(ref _nextMessageId);
            return SendInternalAsync(message, DefaultFrame, ct);
        }

        public Task SendAsync(string text, CancellationToken ct = default)
            => SendAsync(new DuplexMessage(text), ct);

        public Task SendAsync(byte[] data, CancellationToken ct = default)
            => SendAsync(new DuplexMessage(data), ct);

        public async Task<DuplexMessage> SendAndReceiveAsync(DuplexMessage message, CancellationToken ct = default)
        {
            message.Type = MessageType.Request;
            message.Id = Interlocked.Increment(ref _nextMessageId);

            var tcs = new TaskCompletionSource<DuplexMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingRequests[message.Id] = tcs;
            try
            {
                using var registration = ct.Register(() => tcs.TrySetCanceled());
                await SendInternalAsync(message, DefaultFrame, ct).ConfigureAwait(false);
                return await tcs.Task.ConfigureAwait(false);
            }
            finally { _pendingRequests.TryRemove(message.Id, out _); }
        }

        public Task<DuplexMessage> SendAndReceiveAsync(string text, CancellationToken ct = default)
            => SendAndReceiveAsync(new DuplexMessage(text), ct);

        public Task ReplyAsync(DuplexMessage request, DuplexMessage response, CancellationToken ct = default)
        {
            response.Type = MessageType.Response;
            response.Id = request.Id;
            return SendInternalAsync(response, DefaultFrame, ct);
        }

        public Task ReplyAsync(DuplexMessage request, string text, CancellationToken ct = default)
            => ReplyAsync(request, new DuplexMessage(text), ct);

        public async Task CloseAsync()
        {
            if (_disposed) return;
            try { await SendCloseFrameAsync().ConfigureAwait(false); } catch { }
            try { _tcpClient.Close(); } catch { }
            NotifyDisconnected();
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            try { _stream?.Dispose(); } catch { }
            try { _tcpClient?.Close(); } catch { }
            _sendLock?.Dispose();

            foreach (var kvp in _pendingRequests) kvp.Value.TrySetCanceled();
            _pendingRequests.Clear();
        }

        #endregion

        #region TypedPayload / フレーム種別 対応

        public Task SendAsync(TypedPayload payload, CancellationToken ct = default)
            => SendAsync(payload.ToMessage(), ct);

        public async Task<TypedPayload> SendAndReceiveAsync(TypedPayload payload, CancellationToken ct = default)
        {
            var response = await SendAndReceiveAsync(payload.ToMessage(), ct).ConfigureAwait(false);
            return response.ToTypedPayload();
        }

        public Task ReplyAsync(DuplexMessage request, TypedPayload payload, CancellationToken ct = default)
            => ReplyAsync(request, payload.ToMessage(), ct);

        public Task SendAsync(DuplexMessage message, WebSocketFrameKind kind, CancellationToken ct = default)
        {
            message.Type = MessageType.Push;
            message.Id = Interlocked.Increment(ref _nextMessageId);
            return SendInternalAsync(message, kind, ct);
        }

        public Task SendAsync(byte[] data, WebSocketFrameKind kind, CancellationToken ct = default)
            => SendAsync(TypedPayload.FromBinary(data).ToMessage(), kind, ct);

        public async Task<DuplexMessage> SendAndReceiveAsync(DuplexMessage message, WebSocketFrameKind kind, CancellationToken ct = default)
        {
            message.Type = MessageType.Request;
            message.Id = Interlocked.Increment(ref _nextMessageId);

            var tcs = new TaskCompletionSource<DuplexMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingRequests[message.Id] = tcs;
            try
            {
                using var registration = ct.Register(() => tcs.TrySetCanceled());
                await SendInternalAsync(message, kind, ct).ConfigureAwait(false);
                return await tcs.Task.ConfigureAwait(false);
            }
            finally { _pendingRequests.TryRemove(message.Id, out _); }
        }

        public Task ReplyAsync(DuplexMessage request, DuplexMessage response, WebSocketFrameKind kind, CancellationToken ct = default)
        {
            response.Type = MessageType.Response;
            response.Id = request.Id;
            return SendInternalAsync(response, kind, ct);
        }

        #endregion

        #region 受信ループ

        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested && IsConnected)
                {
                    var msg = await ReadMessageAsync(ct).ConfigureAwait(false);
                    if (msg == null) break; // Close または切断

                    if (msg.Value.isText)
                    {
                        var json = Encoding.UTF8.GetString(msg.Value.data);
                        HandleJsonMessage(json);
                    }
                    else
                    {
                        HandleBinaryMessage(msg.Value.data);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (IOException) { }
            catch (Exception ex)
            {
                UnityEngine.Debug.LogWarning($"[TcpDuplexServerChannel] receive error: {ex.Message}");
            }
            finally
            {
                if (!_disposed) NotifyDisconnected();
                foreach (var kvp in _pendingRequests) kvp.Value.TrySetCanceled();
                _pendingRequests.Clear();
            }
        }

        /// <summary>継続フレームを FIN まで結合して1メッセージを返す。Close/切断は null。</summary>
        private async Task<(bool isText, byte[] data)?> ReadMessageAsync(CancellationToken ct)
        {
            using var ms = new MemoryStream();
            bool isText = true;
            bool started = false;

            while (true)
            {
                var frame = await ReadFrameAsync(ct).ConfigureAwait(false);
                if (frame == null) return null;

                int opcode = frame.Value.opcode;

                if (opcode == 0x08) return null;              // Close
                if (opcode == 0x09)                            // Ping → Pong
                {
                    await SendControlAsync(0x8A, frame.Value.payload, ct).ConfigureAwait(false);
                    continue;
                }
                if (opcode == 0x0A) continue;                  // Pong 無視

                if (opcode == 0x01) { isText = true;  started = true; }
                else if (opcode == 0x02) { isText = false; started = true; }
                else if (opcode == 0x00) { if (!started) return null; } // 継続
                else continue;                                 // 未知 opcode 無視

                ms.Write(frame.Value.payload, 0, frame.Value.payload.Length);

                if (frame.Value.fin) return (isText, ms.ToArray());
            }
        }

        /// <summary>1フレームを読む（マスク解除込み）。切断時 null。</summary>
        private async Task<(bool fin, int opcode, byte[] payload)?> ReadFrameAsync(CancellationToken ct)
        {
            byte[] header = new byte[2];
            if (!await ReadExactAsync(header, 0, 2, ct).ConfigureAwait(false)) return null;

            bool fin = (header[0] & 0x80) != 0;
            int opcode = header[0] & 0x0F;
            bool masked = (header[1] & 0x80) != 0;
            long payloadLen = header[1] & 0x7F;

            if (payloadLen == 126)
            {
                byte[] ext = new byte[2];
                if (!await ReadExactAsync(ext, 0, 2, ct).ConfigureAwait(false)) return null;
                payloadLen = (ext[0] << 8) | ext[1];
            }
            else if (payloadLen == 127)
            {
                byte[] ext = new byte[8];
                if (!await ReadExactAsync(ext, 0, 8, ct).ConfigureAwait(false)) return null;
                payloadLen = 0;
                for (int i = 0; i < 8; i++) payloadLen = (payloadLen << 8) | ext[i];
            }

            byte[] maskKey = null;
            if (masked)
            {
                maskKey = new byte[4];
                if (!await ReadExactAsync(maskKey, 0, 4, ct).ConfigureAwait(false)) return null;
            }

            if (payloadLen > 1_000_000_000L) return null; // 1GB 上限
            byte[] payload = new byte[payloadLen];
            if (payloadLen > 0 && !await ReadExactAsync(payload, 0, (int)payloadLen, ct).ConfigureAwait(false))
                return null;

            if (masked && maskKey != null)
                for (int i = 0; i < payload.Length; i++) payload[i] ^= maskKey[i % 4];

            return (fin, opcode, payload);
        }

        private async Task<bool> ReadExactAsync(byte[] buf, int offset, int count, CancellationToken ct)
        {
            int total = 0;
            while (total < count)
            {
                int read = await _stream.ReadAsync(buf, offset + total, count - total, ct).ConfigureAwait(false);
                if (read <= 0) return false;
                total += read;
            }
            return true;
        }

        #endregion

        #region 送信

        private async Task SendInternalAsync(DuplexMessage message, WebSocketFrameKind kind, CancellationToken ct)
        {
            await _sendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                if (kind == WebSocketFrameKind.Binary)
                {
                    var packet = DuplexPacket.Serialize(message);
                    await SendRawAsync(0x82, packet).ConfigureAwait(false);
                }
                else
                {
                    var payload = message.ToTypedPayload();
                    var json = SerializeToJson(message, payload);
                    var bytes = Encoding.UTF8.GetBytes(json);
                    await SendRawAsync(0x81, bytes).ConfigureAwait(false);
                }
            }
            finally { _sendLock.Release(); }
        }

        /// <summary>サーバ→クライアントフレームを送信（マスクなし）。</summary>
        private async Task SendRawAsync(byte opcodeWithFin, byte[] payload)
        {
            payload ??= Array.Empty<byte>();
            byte[] frame;
            if (payload.Length < 126)
            {
                frame = new byte[2 + payload.Length];
                frame[0] = opcodeWithFin;
                frame[1] = (byte)payload.Length;
                Array.Copy(payload, 0, frame, 2, payload.Length);
            }
            else if (payload.Length <= 65535)
            {
                frame = new byte[4 + payload.Length];
                frame[0] = opcodeWithFin;
                frame[1] = 126;
                frame[2] = (byte)(payload.Length >> 8);
                frame[3] = (byte)(payload.Length & 0xFF);
                Array.Copy(payload, 0, frame, 4, payload.Length);
            }
            else
            {
                frame = new byte[10 + payload.Length];
                frame[0] = opcodeWithFin;
                frame[1] = 127;
                long len = payload.Length;
                for (int i = 7; i >= 0; i--) { frame[2 + i] = (byte)(len & 0xFF); len >>= 8; }
                Array.Copy(payload, 0, frame, 10, payload.Length);
            }
            await _stream.WriteAsync(frame, 0, frame.Length).ConfigureAwait(false);
        }

        private Task SendCloseFrameAsync() => SendControlAsync(0x88, Array.Empty<byte>());

        /// <summary>制御フレーム（Pong/Close）を送信ロック経由で送る（データ送信と直列化）。</summary>
        private async Task SendControlAsync(byte opcode, byte[] payload, CancellationToken ct = default)
        {
            await _sendLock.WaitAsync(ct).ConfigureAwait(false);
            try { await SendRawAsync(opcode, payload).ConfigureAwait(false); }
            finally { _sendLock.Release(); }
        }

        #endregion

        #region メッセージマッピング（WebSocketDuplexChannel と同一仕様）

        private void HandleJsonMessage(string json)
        {
            try
            {
                var root = JObject.Parse(json);
                var msgType = root["type"]?.ToString() ?? "send";

                // envelope の id は数値。旧クライアントは文字列 id（"pc_1"等）を送るため、
                // 整数トークンのときのみ int 化し、それ以外は 0（例外を投げない）。
                int id = 0;
                var idTok = root["id"];
                if (idTok != null && idTok.Type == JTokenType.Integer)
                    id = idTok.ToObject<int>();

                TypedPayload payload;
                var itemsEl = root["items"];
                if (itemsEl != null && itemsEl.Type == JTokenType.Array)
                    payload = ParseItemsFromJson((JArray)itemsEl);
                else if (root["text"] != null)
                    payload = TypedPayload.FromText(root["text"].ToString());
                else if (root["json"] != null)
                    payload = TypedPayload.FromJson(root["json"].ToString());
                else
                    // envelope でない生アプリ JSON はフレーム全体を Json ペイロードとして扱い、
                    // RemoteServerCore.OnDuplexReceived の Json アイテム → ProcessMessage へ渡す。
                    payload = TypedPayload.FromJson(json);

                var message = new DuplexMessage
                {
                    Id = id,
                    Type = msgType == "request" ? MessageType.Request :
                           msgType == "response" ? MessageType.Response : MessageType.Push,
                    Payload = payload.Serialize()
                };
                HandleMessage(message);
            }
            catch (Exception ex)
            {
                UnityEngine.Debug.LogWarning($"[TcpDuplexServerChannel] JSON parse error: {ex.Message}");
            }
        }

        private TypedPayload ParseItemsFromJson(JArray itemsEl)
        {
            var payload = new TypedPayload();
            foreach (var item in itemsEl)
            {
                var type = item["type"]?.ToObject<int>() ?? 0;
                var mimeType = item["mimeType"]?.ToString() ?? "";
                var data = item["data"]?.ToString() ?? "";
                var encoding = item["encoding"]?.ToString() ?? "";

                byte[] bytes = encoding == "base64"
                    ? Convert.FromBase64String(data)
                    : Encoding.UTF8.GetBytes(data);

                switch ((ContentType)type)
                {
                    case ContentType.Text:   payload.AddText(data); break;
                    case ContentType.Json:   payload.AddJson(data); break;
                    case ContentType.Image:  payload.AddImage(bytes, mimeType ?? "image/png"); break;
                    case ContentType.Binary: payload.AddBinary(bytes, mimeType ?? "application/octet-stream"); break;
                    default:                 payload.AddCustom(bytes, mimeType ?? "application/octet-stream"); break;
                }
            }
            return payload;
        }

        private void HandleBinaryMessage(byte[] data)
        {
            if (data.Length < DuplexPacket.HeaderSize) return;

            var header = new byte[DuplexPacket.HeaderSize];
            Buffer.BlockCopy(data, 0, header, 0, DuplexPacket.HeaderSize);

            if (!DuplexPacket.TryParseHeader(header, out var type, out var messageId, out var payloadLength, out var tagLength))
                return;

            var bodyLength = tagLength + payloadLength;
            var body = new byte[bodyLength];
            if (bodyLength > 0 && data.Length >= DuplexPacket.HeaderSize + bodyLength)
                Buffer.BlockCopy(data, DuplexPacket.HeaderSize, body, 0, bodyLength);

            var message = DuplexPacket.ParseBody(type, messageId, body, tagLength);
            HandleMessage(message);
        }

        private void HandleMessage(DuplexMessage message)
        {
            if (message.Type == MessageType.Response)
            {
                if (_pendingRequests.TryRemove(message.Id, out var tcs))
                    tcs.TrySetResult(message);
            }
            else
            {
                OnReceived?.Invoke(this, message);
            }
        }

        private string SerializeToJson(DuplexMessage message, TypedPayload payload)
        {
            var sb = new StringBuilder();
            sb.Append("{");
            var typeStr = message.Type switch
            {
                MessageType.Push => "push",
                MessageType.Request => "request",
                MessageType.Response => "response",
                _ => "push"
            };
            sb.Append($"\"type\":\"{typeStr}\",");
            sb.Append($"\"id\":{message.Id},");
            sb.Append("\"items\":[");
            var first = true;
            foreach (var item in payload)
            {
                if (!first) sb.Append(",");
                first = false;
                sb.Append("{");
                sb.Append($"\"type\":{(int)item.Type},");
                sb.Append($"\"mimeType\":\"{EscapeJson(item.MimeType)}\",");
                if (item.Type == ContentType.Text || item.Type == ContentType.Json)
                    sb.Append($"\"data\":\"{EscapeJson(item.DataString ?? "")}\"");
                else
                {
                    sb.Append($"\"data\":\"{Convert.ToBase64String(item.Data ?? Array.Empty<byte>())}\",");
                    sb.Append("\"encoding\":\"base64\"");
                }
                sb.Append("}");
            }
            sb.Append("]");
            sb.Append("}");
            return sb.ToString();
        }

        private static string EscapeJson(string s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            return s.Replace("\\", "\\\\")
                    .Replace("\"", "\\\"")
                    .Replace("\n", "\\n")
                    .Replace("\r", "\\r")
                    .Replace("\t", "\\t");
        }

        private void NotifyDisconnected()
        {
            try { OnDisconnected?.Invoke(this); } catch { }
        }

        #endregion
    }
}
