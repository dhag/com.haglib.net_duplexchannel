using System;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// 名前付きパイプ双方向通信クライアント
    /// </summary>
    public class PipeDuplexClient : IDuplexChannel
    {
        private PipeDuplexChannel _channel;
        private readonly string _pipeName;
        private bool _disposed;

        public bool IsConnected => _channel?.IsConnected ?? false;
        public string Id => _channel?.Id ?? "";

        public event Action<IDuplexChannel, DuplexMessage> OnReceived;
        public event Action<IDuplexChannel> OnDisconnected;

        /// <summary>
        /// コンストラクタ
        /// </summary>
        /// <param name="pipeName">パイプ名</param>
        public PipeDuplexClient(string pipeName)
        {
            _pipeName = pipeName ?? throw new ArgumentNullException(nameof(pipeName));
        }

        /// <summary>
        /// サーバーに接続
        /// </summary>
        public async Task ConnectAsync(int timeoutMs = 5000, CancellationToken ct = default)
        {
            if (_channel != null)
                throw new InvalidOperationException("Already connected.");

            _channel = new PipeDuplexChannel();
            _channel.OnReceived += (ch, msg) => OnReceived?.Invoke(this, msg);
            _channel.OnDisconnected += (ch) => OnDisconnected?.Invoke(this);

            await _channel.ConnectAsync(_pipeName, timeoutMs, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// 再接続
        /// </summary>
        public async Task ReconnectAsync(int timeoutMs = 5000, CancellationToken ct = default)
        {
            if (_channel != null)
            {
                _channel.Dispose();
                _channel = null;
            }

            await ConnectAsync(timeoutMs, ct).ConfigureAwait(false);
        }

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

        public Task CloseAsync()
        {
            return _channel?.CloseAsync() ?? Task.CompletedTask;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _channel?.Dispose();
        }

        private void EnsureConnected()
        {
            if (_channel == null || !_channel.IsConnected)
                throw new InvalidOperationException("Not connected.");
        }
    }
}
