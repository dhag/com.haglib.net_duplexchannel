using System;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Pipes;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// 名前付きパイプ双方向通信チャネル
    /// </summary>
    public class PipeDuplexChannel : IDuplexChannel
    {
        private PipeStream _pipeStream;
        private readonly CancellationTokenSource _cts;
        private readonly ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>> _pendingRequests;
        private readonly SemaphoreSlim _sendLock;
        private int _nextMessageId;
        private bool _disposed;
        private Task _receiveTask;

        public bool IsConnected => _pipeStream?.IsConnected ?? false;
        public string Id { get; }

        public event Action<IDuplexChannel, DuplexMessage> OnReceived;
        public event Action<IDuplexChannel> OnDisconnected;

        /// <summary>
        /// 既存のパイプストリームからチャネルを作成（サーバー用）
        /// </summary>
        internal PipeDuplexChannel(PipeStream pipeStream, string id = null)
        {
            _pipeStream = pipeStream ?? throw new ArgumentNullException(nameof(pipeStream));
            _cts = new CancellationTokenSource();
            _pendingRequests = new ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>>();
            _sendLock = new SemaphoreSlim(1, 1);
            _nextMessageId = 0;
            Id = id ?? Guid.NewGuid().ToString("N").Substring(0, 8);
        }

        /// <summary>
        /// クライアント用コンストラクタ
        /// </summary>
        public PipeDuplexChannel() 
        {
            _cts = new CancellationTokenSource();
            _pendingRequests = new ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>>();
            _sendLock = new SemaphoreSlim(1, 1);
            _nextMessageId = 0;
            Id = Guid.NewGuid().ToString("N").Substring(0, 8);
        }

        /// <summary>
        /// サーバーに接続（クライアント用）
        /// </summary>
        public async Task ConnectAsync(string pipeName, int timeoutMs = 5000, CancellationToken ct = default)
        {
            var clientStream = new NamedPipeClientStream(
                ".",
                pipeName,
                PipeDirection.InOut,
                PipeOptions.Asynchronous);

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
            
            await clientStream.ConnectAsync(timeoutMs, linkedCts.Token).ConfigureAwait(false);
            
            _pipeStream = clientStream;
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
            response.Id = request.Id;
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

            ClosePipe();
            OnDisconnected?.Invoke(this);
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _cts.Cancel();
            ClosePipe();
            _cts.Dispose();
            _sendLock?.Dispose();
        }

        private void ClosePipe()
        {
            try
            {
                _pipeStream?.Close();
                _pipeStream?.Dispose();
                _pipeStream = null;
            }
            catch { }
        }

        private async Task SendInternalAsync(DuplexMessage message, CancellationToken ct)
        {
            await _sendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var packet = DuplexPacket.Serialize(message);
                await _pipeStream.WriteAsync(packet, 0, packet.Length, ct).ConfigureAwait(false);
                await _pipeStream.FlushAsync(ct).ConfigureAwait(false);
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
                while (!ct.IsCancellationRequested && _pipeStream != null && _pipeStream.IsConnected)
                {
                    // ヘッダー読み取り
                    if (!await ReadExactAsync(headerBuffer, 0, DuplexPacket.HeaderSize, ct).ConfigureAwait(false))
                        break;

                    if (!DuplexPacket.TryParseHeader(headerBuffer, out var type, out var messageId, out var payloadLength, out var tagLength))
                    {
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
                    HandleMessage(message);
                }
            }
            catch (OperationCanceledException) { }
            catch (IOException) { } // パイプ切断
            catch (Exception) { }
            finally
            {
                if (!_disposed)
                {
                    OnDisconnected?.Invoke(this);
                }

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
                var read = await _pipeStream.ReadAsync(buffer, offset + totalRead, count - totalRead, ct).ConfigureAwait(false);
                if (read == 0)
                    return false;
                totalRead += read;
            }
            return true;
        }

        private void HandleMessage(DuplexMessage message)
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
                OnReceived?.Invoke(this, message);
            }
        }
    }
}
