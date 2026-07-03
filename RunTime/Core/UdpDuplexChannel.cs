using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// UDP双方向通信チャネル
    /// 
    /// 注意:
    /// - コネクションレス（接続状態なし）
    /// - パケットロス・順序逆転の可能性あり
    /// - 1パケット最大約1400バイト推奨（MTU制限）
    /// - SendAndReceiveAsync は応答が届かない可能性あり（タイムアウトあり）
    /// </summary>
    public class UdpDuplexChannel : IDuplexChannel
    {
        public const int MaxPayloadSize = 1400;
        public const int DefaultTimeoutMs = 3000;

        private UdpClient _udpClient;
        private readonly CancellationTokenSource _cts;
        private readonly ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>> _pendingRequests;
        private readonly SemaphoreSlim _sendLock;
        private int _nextMessageId;
        private bool _disposed;
        private Task _receiveTask;
        private IPEndPoint _remoteEndPoint;
        private IPEndPoint _localEndPoint;
        private int _timeoutMs = DefaultTimeoutMs;

        /// <summary>受信時の送信元アドレス（ReplyAsync用）</summary>
        public IPEndPoint LastReceivedFrom { get; private set; }

        /// <summary>デフォルトタイムアウト（ミリ秒）</summary>
        public int TimeoutMs
        {
            get => _timeoutMs;
            set => _timeoutMs = value > 0 ? value : DefaultTimeoutMs;
        }

        public string Id { get; }

        /// <summary>接続先が設定されているか（UDPは常にtrue扱い）</summary>
        public bool IsConnected => _udpClient != null && _remoteEndPoint != null;

        public event Action<IDuplexChannel, DuplexMessage> OnReceived;
        public event Action<IDuplexChannel> OnDisconnected;

        /// <summary>メッセージ受信イベント（送信元アドレス付き）</summary>
        public event Action<UdpDuplexChannel, DuplexMessage, IPEndPoint> OnReceivedWithEndPoint;

        /// <summary>
        /// コンストラクタ
        /// </summary>
        public UdpDuplexChannel()
        {
            _cts = new CancellationTokenSource();
            _pendingRequests = new ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>>();
            _sendLock = new SemaphoreSlim(1, 1);
            _nextMessageId = 0;
            Id = Guid.NewGuid().ToString("N").Substring(0, 8);
        }

        /// <summary>
        /// 受信用にバインド（サーバー的な使い方）
        /// </summary>
        public void Bind(int port)
        {
            Bind(new IPEndPoint(IPAddress.Any, port));
        }

        /// <summary>
        /// 受信用にバインド
        /// </summary>
        public void Bind(IPEndPoint localEndPoint)
        {
            _localEndPoint = localEndPoint;
            _udpClient = new UdpClient(localEndPoint);
            StartReceiving();
        }

        /// <summary>
        /// 送信先を設定（クライアント的な使い方）
        /// </summary>
        public void Connect(string host, int port)
        {
            Connect(new IPEndPoint(IPAddress.Parse(host), port));
        }

        /// <summary>
        /// 送信先を設定
        /// </summary>
        public void Connect(IPEndPoint remoteEndPoint)
        {
            _remoteEndPoint = remoteEndPoint;
            if (_udpClient == null)
            {
                _udpClient = new UdpClient();
            }
            StartReceiving();
        }

        /// <summary>
        /// バインドと送信先設定を両方行う
        /// </summary>
        public void BindAndConnect(int localPort, string remoteHost, int remotePort)
        {
            _localEndPoint = new IPEndPoint(IPAddress.Any, localPort);
            _remoteEndPoint = new IPEndPoint(IPAddress.Parse(remoteHost), remotePort);
            _udpClient = new UdpClient(_localEndPoint);
            StartReceiving();
        }

        private void StartReceiving()
        {
            if (_receiveTask == null)
            {
                _receiveTask = Task.Run(() => ReceiveLoopAsync(_cts.Token));
            }
        }

        private void ValidatePayloadSize(DuplexMessage message)
        {
            var packet = DuplexPacket.Serialize(message);
            if (packet.Length > MaxPayloadSize)
            {
                throw new ArgumentException($"Message size ({packet.Length} bytes) exceeds UDP MTU limit ({MaxPayloadSize} bytes). Use TCP or WebSocket for large data.");
            }
        }

        #region IDuplexChannel 実装

        /// <summary>
        /// プッシュ送信（デフォルト送信先へ）
        /// </summary>
        public Task SendAsync(DuplexMessage message, CancellationToken ct = default)
        {
            if (_remoteEndPoint == null)
                throw new InvalidOperationException("Remote endpoint not set. Call Connect() first.");
            
            ValidatePayloadSize(message);
            return SendAsyncInternal(message, _remoteEndPoint, ct);
        }

        public Task SendAsync(string text, CancellationToken ct = default)
        {
            return SendAsync(new DuplexMessage(text), ct);
        }

        public Task SendAsync(byte[] data, CancellationToken ct = default)
        {
            return SendAsync(new DuplexMessage(data), ct);
        }

        /// <summary>
        /// リクエスト送信して応答を待つ（デフォルトタイムアウト使用）
        /// </summary>
        public Task<DuplexMessage> SendAndReceiveAsync(DuplexMessage message, CancellationToken ct = default)
        {
            if (_remoteEndPoint == null)
                throw new InvalidOperationException("Remote endpoint not set. Call Connect() first.");
            
            ValidatePayloadSize(message);
            return SendAndReceiveAsync(message, _remoteEndPoint, _timeoutMs, ct);
        }

        public Task<DuplexMessage> SendAndReceiveAsync(string text, CancellationToken ct = default)
        {
            return SendAndReceiveAsync(new DuplexMessage(text), ct);
        }

        /// <summary>
        /// リクエストに応答（LastReceivedFromに返信）
        /// </summary>
        public Task ReplyAsync(DuplexMessage request, DuplexMessage response, CancellationToken ct = default)
        {
            if (LastReceivedFrom == null)
                throw new InvalidOperationException("No message received yet. LastReceivedFrom is null.");
            
            ValidatePayloadSize(response);
            return ReplyAsync(request, response, LastReceivedFrom, ct);
        }

        public Task ReplyAsync(DuplexMessage request, string text, CancellationToken ct = default)
        {
            return ReplyAsync(request, new DuplexMessage(text), ct);
        }

        public Task CloseAsync()
        {
            Close();
            return Task.CompletedTask;
        }

        #endregion

        #region 送信先指定版メソッド（UDP固有）

        /// <summary>
        /// プッシュ送信（送信先指定）
        /// </summary>
        public async Task SendAsync(DuplexMessage message, IPEndPoint remoteEndPoint, CancellationToken ct = default)
        {
            ValidatePayloadSize(message);
            await SendAsyncInternal(message, remoteEndPoint, ct).ConfigureAwait(false);
        }

        private async Task SendAsyncInternal(DuplexMessage message, IPEndPoint remoteEndPoint, CancellationToken ct)
        {
            message.Type = MessageType.Push;
            message.Id = Interlocked.Increment(ref _nextMessageId);
            await SendInternalAsync(message, remoteEndPoint, ct).ConfigureAwait(false);
        }

        public Task SendAsync(string text, IPEndPoint remoteEndPoint, CancellationToken ct = default)
        {
            return SendAsync(new DuplexMessage(text), remoteEndPoint, ct);
        }

        /// <summary>
        /// リクエスト送信して応答を待つ（タイムアウト指定）
        /// </summary>
        public Task<DuplexMessage> SendAndReceiveAsync(DuplexMessage message, int timeoutMs, CancellationToken ct = default)
        {
            if (_remoteEndPoint == null)
                throw new InvalidOperationException("Remote endpoint not set. Call Connect() first.");
            return SendAndReceiveAsync(message, _remoteEndPoint, timeoutMs, ct);
        }

        /// <summary>
        /// リクエスト送信して応答を待つ（送信先指定）
        /// </summary>
        public async Task<DuplexMessage> SendAndReceiveAsync(DuplexMessage message, IPEndPoint remoteEndPoint, int timeoutMs, CancellationToken ct = default)
        {
            ValidatePayloadSize(message);
            
            message.Type = MessageType.Request;
            message.Id = Interlocked.Increment(ref _nextMessageId);

            var tcs = new TaskCompletionSource<DuplexMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingRequests[message.Id] = tcs;

            try
            {
                using var timeoutCts = new CancellationTokenSource(timeoutMs);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token, timeoutCts.Token);
                linkedCts.Token.Register(() => tcs.TrySetCanceled());

                await SendInternalAsync(message, remoteEndPoint, linkedCts.Token).ConfigureAwait(false);
                return await tcs.Task.ConfigureAwait(false);
            }
            finally
            {
                _pendingRequests.TryRemove(message.Id, out _);
            }
        }

        public Task<DuplexMessage> SendAndReceiveAsync(string text, int timeoutMs, CancellationToken ct = default)
        {
            return SendAndReceiveAsync(new DuplexMessage(text), timeoutMs, ct);
        }

        /// <summary>
        /// リクエストに応答（送信先指定）
        /// </summary>
        public Task ReplyAsync(DuplexMessage request, DuplexMessage response, IPEndPoint remoteEndPoint, CancellationToken ct = default)
        {
            ValidatePayloadSize(response);
            response.Type = MessageType.Response;
            response.Id = request.Id;
            return SendInternalAsync(response, remoteEndPoint, ct);
        }

        public Task ReplyAsync(DuplexMessage request, string text, IPEndPoint remoteEndPoint, CancellationToken ct = default)
        {
            return ReplyAsync(request, new DuplexMessage(text), remoteEndPoint, ct);
        }

        /// <summary>
        /// ブロードキャスト送信
        /// </summary>
        public async Task BroadcastAsync(DuplexMessage message, int port, CancellationToken ct = default)
        {
            ValidatePayloadSize(message);
            message.Type = MessageType.Push;
            message.Id = Interlocked.Increment(ref _nextMessageId);
            var broadcastEndPoint = new IPEndPoint(IPAddress.Broadcast, port);
            
            _udpClient.EnableBroadcast = true;
            await SendInternalAsync(message, broadcastEndPoint, ct).ConfigureAwait(false);
        }

        public Task BroadcastAsync(string text, int port, CancellationToken ct = default)
        {
            return BroadcastAsync(new DuplexMessage(text), port, ct);
        }

        #endregion

        public void Close()
        {
            _cts.Cancel();
            _udpClient?.Close();
            OnDisconnected?.Invoke(this);
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _cts.Cancel();
            _udpClient?.Close();
            _udpClient?.Dispose();
            _cts.Dispose();
            _sendLock?.Dispose();
        }

        private async Task SendInternalAsync(DuplexMessage message, IPEndPoint remoteEndPoint, CancellationToken ct)
        {
            await _sendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var packet = DuplexPacket.Serialize(message);
                await _udpClient.SendAsync(packet, packet.Length, remoteEndPoint).ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }

        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var result = await _udpClient.ReceiveAsync().ConfigureAwait(false);
                    
                    if (ct.IsCancellationRequested) break;

                    LastReceivedFrom = result.RemoteEndPoint;
                    var data = result.Buffer;

                    if (data.Length < DuplexPacket.HeaderSize)
                        continue;

                    var header = new byte[DuplexPacket.HeaderSize];
                    Buffer.BlockCopy(data, 0, header, 0, DuplexPacket.HeaderSize);

                    if (!DuplexPacket.TryParseHeader(header, out var type, out var messageId, out var payloadLength, out var tagLength))
                        continue;

                    var bodyLength = tagLength + payloadLength;
                    byte[] body = null;
                    if (bodyLength > 0 && data.Length >= DuplexPacket.HeaderSize + bodyLength)
                    {
                        body = new byte[bodyLength];
                        Buffer.BlockCopy(data, DuplexPacket.HeaderSize, body, 0, bodyLength);
                    }

                    var message = DuplexPacket.ParseBody(type, messageId, body ?? Array.Empty<byte>(), tagLength);
                    HandleMessage(message, result.RemoteEndPoint);
                }
            }
            catch (ObjectDisposedException) { }
            catch (SocketException) { }
            catch (OperationCanceledException) { }
            finally
            {
                foreach (var kvp in _pendingRequests)
                {
                    kvp.Value.TrySetCanceled();
                }
                _pendingRequests.Clear();
            }
        }

        private void HandleMessage(DuplexMessage message, IPEndPoint from)
        {
            if (message.Type == MessageType.Response)
            {
                if (_pendingRequests.TryRemove(message.Id, out var tcs))
                {
                    tcs.TrySetResult(message);
                }
            }
            else
            {
                // IDuplexChannel用（送信元なし）
                OnReceived?.Invoke(this, message);
                // UDP固有（送信元あり）
                OnReceivedWithEndPoint?.Invoke(this, message, from);
            }
        }
    }
}
