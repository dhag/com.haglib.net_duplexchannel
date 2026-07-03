using System;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// WebSocket双方向通信クライアント
    /// 内部でWebSocketDuplexChannelを使用
    /// </summary>
    public class WebSocketDuplexClient : IDuplexChannel
    {
        private ClientWebSocket _webSocket;
        private WebSocketDuplexChannel _channel;
        private CancellationTokenSource _cts;
        private bool _disposed;

        public bool IsConnected => _channel?.IsConnected ?? false;
        public string Id => _channel?.Id ?? "";

        /// <summary>kind省略時に使用する既定の送信フレーム種別（接続時にChannelへ伝播）</summary>
        public WebSocketFrameKind DefaultFrame { get; set; } = WebSocketFrameKind.Text;
        
        /// <summary>
        /// 内部のチャネルを取得
        /// </summary>
        public WebSocketDuplexChannel Channel => _channel;

        public event Action<IDuplexChannel, DuplexMessage> OnReceived;
        public event Action<IDuplexChannel> OnDisconnected;

        /// <summary>
        /// サーバーに接続
        /// </summary>
        public async Task ConnectAsync(string uri, CancellationToken ct = default)
        {
            await ConnectAsync(new Uri(uri), ct).ConfigureAwait(false);
        }

        /// <summary>
        /// サーバーに接続
        /// </summary>
        public async Task ConnectAsync(Uri uri, CancellationToken ct = default)
        {
            if (_channel != null && _channel.IsConnected)
                throw new InvalidOperationException("Already connected.");

            _webSocket?.Dispose();
            _channel?.Dispose();

            _webSocket = new ClientWebSocket();
            _cts = new CancellationTokenSource();

            await _webSocket.ConnectAsync(uri, ct).ConfigureAwait(false);

            _channel = new WebSocketDuplexChannel(_webSocket);
            _channel.DefaultFrame = DefaultFrame;
            _channel.OnReceived += (ch, msg) => OnReceived?.Invoke(this, msg);
            _channel.OnDisconnected += (ch) => OnDisconnected?.Invoke(this);
            _channel.StartReceiving(_cts.Token);
        }

        /// <summary>
        /// 再接続
        /// </summary>
        public async Task ReconnectAsync(Uri uri, CancellationToken ct = default)
        {
            await CloseAsync().ConfigureAwait(false);
            await ConnectAsync(uri, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// 再接続
        /// </summary>
        public async Task ReconnectAsync(string uri, CancellationToken ct = default)
        {
            await ReconnectAsync(new Uri(uri), ct).ConfigureAwait(false);
        }

        #region IDuplexChannel 実装

        public Task SendAsync(DuplexMessage message, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.SendAsync(message, ct);
        }

        public Task SendAsync(string text, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.SendAsync(text, ct);
        }

        public Task SendAsync(byte[] data, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.SendAsync(data, ct);
        }

        public Task<DuplexMessage> SendAndReceiveAsync(DuplexMessage message, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.SendAndReceiveAsync(message, ct);
        }

        public Task<DuplexMessage> SendAndReceiveAsync(string text, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.SendAndReceiveAsync(text, ct);
        }

        public Task ReplyAsync(DuplexMessage request, DuplexMessage response, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.ReplyAsync(request, response, ct);
        }

        public Task ReplyAsync(DuplexMessage request, string text, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.ReplyAsync(request, text, ct);
        }

        public async Task CloseAsync()
        {
            if (_disposed) return;

            _cts?.Cancel();

            if (_channel != null)
            {
                await _channel.CloseAsync().ConfigureAwait(false);
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _cts?.Cancel();
            _channel?.Dispose();
            _webSocket?.Dispose();
            _cts?.Dispose();
        }

        #endregion

        #region 追加メソッド（TypedPayload対応）

        public Task SendAsync(TypedPayload payload, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.SendAsync(payload, ct);
        }

        public async Task<TypedPayload> SendAndReceiveAsync(TypedPayload payload, CancellationToken ct = default)
        {
            EnsureConnected();
            return await _channel.SendAndReceiveAsync(payload, ct).ConfigureAwait(false);
        }

        public Task ReplyAsync(DuplexMessage request, TypedPayload payload, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.ReplyAsync(request, payload, ct);
        }

        #endregion

        #region 追加メソッド（フレーム種別指定：Text/Binary）

        public Task SendAsync(DuplexMessage message, WebSocketFrameKind kind, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.SendAsync(message, kind, ct);
        }

        public Task SendAsync(byte[] data, WebSocketFrameKind kind, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.SendAsync(data, kind, ct);
        }

        public Task<DuplexMessage> SendAndReceiveAsync(DuplexMessage message, WebSocketFrameKind kind, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.SendAndReceiveAsync(message, kind, ct);
        }

        public Task ReplyAsync(DuplexMessage request, DuplexMessage response, WebSocketFrameKind kind, CancellationToken ct = default)
        {
            EnsureConnected();
            return _channel.ReplyAsync(request, response, kind, ct);
        }

        #endregion

        private void EnsureConnected()
        {
            if (_channel == null || !_channel.IsConnected)
                throw new InvalidOperationException("Not connected.");
        }
    }
}
