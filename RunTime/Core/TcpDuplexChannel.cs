using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// TCP双方向通信チャネル
    /// </summary>
    public class TcpDuplexChannel : IDuplexChannel
    {
        private Socket _socket;
        private NetworkStream _stream;
        private readonly CancellationTokenSource _cts;
        private readonly ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>> _pendingRequests;
        private readonly SemaphoreSlim _sendLock; // 送信の排他制御
        private int _nextMessageId;
        private bool _disposed;
        private Task _receiveTask;

        public bool IsConnected => _socket?.Connected ?? false;
        public string Id { get; }

        public event Action<IDuplexChannel, DuplexMessage> OnReceived;
        public event Action<IDuplexChannel> OnDisconnected;

        /// <summary>
        /// 既存のソケットからチャネルを作成（サーバー用、既に接続済み）
        /// </summary>
        internal TcpDuplexChannel(Socket socket, string id = null)
        {
            _socket = socket ?? throw new ArgumentNullException(nameof(socket));
            _stream = new NetworkStream(socket, ownsSocket: false);
            _cts = new CancellationTokenSource();
            _pendingRequests = new ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>>();
            _sendLock = new SemaphoreSlim(1, 1);
            _nextMessageId = 0;
            Id = id ?? Guid.NewGuid().ToString("N").Substring(0, 8);
        }

        /// <summary>
        /// 新規接続用（クライアント用）
        /// </summary>
        public TcpDuplexChannel()
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _stream = null;  // ConnectAsync で作成
            _cts = new CancellationTokenSource();
            _pendingRequests = new ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>>();
            _sendLock = new SemaphoreSlim(1, 1);
            _nextMessageId = 0;
            Id = Guid.NewGuid().ToString("N").Substring(0, 8);
        }

        /// <summary>
        /// サーバーに接続
        /// </summary>
        public async Task ConnectAsync(string host, int port, CancellationToken ct = default)
        {
            await _socket.ConnectAsync(host, port).ConfigureAwait(false);
            _stream = new NetworkStream(_socket, ownsSocket: false);
            StartReceiving();
        }

        /// <summary>
        /// 受信処理を開始
        /// </summary>
        internal void StartReceiving()
        {
            _receiveTask = Task.Run(() => ReceiveLoopAsync(_cts.Token));
        }

        /// <summary>
        /// プッシュ送信
        /// </summary>
        public Task SendAsync(DuplexMessage message, CancellationToken ct = default)
        {
            message.Type = MessageType.Push;
            message.Id = Interlocked.Increment(ref _nextMessageId);
            return SendInternalAsync(message, ct);
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
        /// リクエスト送信して応答を待つ
        /// </summary>
        public async Task<DuplexMessage> SendAndReceiveAsync(DuplexMessage message, CancellationToken ct = default)
        {
            message.Type = MessageType.Request;
            message.Id = Interlocked.Increment(ref _nextMessageId);

            var tcs = new TaskCompletionSource<DuplexMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingRequests[message.Id] = tcs;

            try
            {
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
                linkedCts.Token.Register(() => tcs.TrySetCanceled());

                await SendInternalAsync(message, linkedCts.Token).ConfigureAwait(false);
                return await tcs.Task.ConfigureAwait(false);
            }
            finally
            {
                _pendingRequests.TryRemove(message.Id, out _);
            }
        }

        public Task<DuplexMessage> SendAndReceiveAsync(string text, CancellationToken ct = default)
        {
            return SendAndReceiveAsync(new DuplexMessage(text), ct);
        }

        /// <summary>
        /// リクエストに応答
        /// </summary>
        public Task ReplyAsync(DuplexMessage request, DuplexMessage response, CancellationToken ct = default)
        {
            response.Type = MessageType.Response;
            response.Id = request.Id; // 同じIDで返す
            return SendInternalAsync(response, ct);
        }

        public Task ReplyAsync(DuplexMessage request, string text, CancellationToken ct = default)
        {
            return ReplyAsync(request, new DuplexMessage(text), ct);
        }

        /// <summary>
        /// 切断
        /// </summary>
        public async Task CloseAsync()
        {
            if (_disposed) return;

            _cts.Cancel();

            try
            {
                if (_receiveTask != null)
                {
                    await _receiveTask.ConfigureAwait(false);
                }
            }
            catch { }

            CloseSocket();
            OnDisconnected?.Invoke(this);
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _cts.Cancel();
            CloseSocket();
            _cts.Dispose();
            _stream?.Dispose();
            _sendLock?.Dispose();
        }

        private void CloseSocket()
        {
            try
            {
                if (_socket != null && _socket.Connected)
                {
                    _socket.Shutdown(SocketShutdown.Both);
                }
            }
            catch { }

            try
            {
                _socket?.Close();
            }
            catch { }
        }

        private async Task SendInternalAsync(DuplexMessage message, CancellationToken ct)
        {
            await _sendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var packet = DuplexPacket.Serialize(message);
                await _stream.WriteAsync(packet, 0, packet.Length, ct).ConfigureAwait(false);
                await _stream.FlushAsync(ct).ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }

        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            var headerBuffer = new byte[DuplexPacket.HeaderSize];

            try
            {
                while (!ct.IsCancellationRequested && _socket.Connected)
                {
                    // ヘッダー読み取り
                    if (!await ReadExactAsync(headerBuffer, 0, DuplexPacket.HeaderSize, ct).ConfigureAwait(false))
                        break;

                    if (!DuplexPacket.TryParseHeader(headerBuffer, out var type, out var messageId, out var payloadLength, out var tagLength))
                    {
                        // 不正なパケット - 接続を切る
                        break;
                    }

                    // ボディ読み取り
                    var bodyLength = tagLength + payloadLength;
                    byte[] bodyBuffer = null;
                    if (bodyLength > 0)
                    {
                        bodyBuffer = new byte[bodyLength];
                        if (!await ReadExactAsync(bodyBuffer, 0, bodyLength, ct).ConfigureAwait(false))
                            break;
                    }

                    var message = DuplexPacket.ParseBody(type, messageId, bodyBuffer ?? Array.Empty<byte>(), tagLength);

                    // メッセージ処理
                    HandleMessage(message);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception) { }
            finally
            {
                if (!_disposed)
                {
                    OnDisconnected?.Invoke(this);
                }

                // 保留中のリクエストをキャンセル
                foreach (var kvp in _pendingRequests)
                {
                    kvp.Value.TrySetCanceled();
                }
                _pendingRequests.Clear();
            }
        }

        private async Task<bool> ReadExactAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            var totalRead = 0;
            while (totalRead < count)
            {
                var read = await _stream.ReadAsync(buffer, offset + totalRead, count - totalRead, ct).ConfigureAwait(false);
                if (read == 0)
                    return false; // 接続切断
                totalRead += read;
            }
            return true;
        }

        private void HandleMessage(DuplexMessage message)
        {
            if (message.Type == MessageType.Response)
            {
                // リクエストへの応答
                if (_pendingRequests.TryRemove(message.Id, out var tcs))
                {
                    tcs.TrySetResult(message);
                }
            }
            else
            {
                // プッシュまたはリクエスト → イベント発火
                OnReceived?.Invoke(this, message);
            }
        }
    }
}