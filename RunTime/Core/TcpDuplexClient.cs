using System;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// TCP双方向通信クライアント
    /// TcpDuplexChannel のラッパーで、接続管理を簡略化
    /// </summary>
    public class TcpDuplexClient : IDuplexChannel
    {
        private TcpDuplexChannel _channel;
        private readonly string _host;
        private readonly int _port;
        private bool _disposed;

        public bool IsConnected => _channel?.IsConnected ?? false;
        public string Id => _channel?.Id ?? "";

        public event Action<IDuplexChannel, DuplexMessage> OnReceived;
        public event Action<IDuplexChannel> OnDisconnected;

        /// <summary>
        /// コンストラクタ
        /// </summary>
        public TcpDuplexClient(string host, int port)
        {
            _host = host;
            _port = port;
        }

        /// <summary>
        /// サーバーに接続
        /// </summary>
        public async Task ConnectAsync(CancellationToken ct = default)
        {
            if (_channel != null)
                throw new InvalidOperationException("Already connected.");

            _channel = new TcpDuplexChannel();
            _channel.OnReceived += (ch, msg) => OnReceived?.Invoke(this, msg);
            _channel.OnDisconnected += (ch) => OnDisconnected?.Invoke(this);

            await _channel.ConnectAsync(_host, _port, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// 再接続
        /// </summary>
        public async Task ReconnectAsync(CancellationToken ct = default)
        {
            if (_channel != null)
            {
                _channel.Dispose();
                _channel = null;
            }

            await ConnectAsync(ct).ConfigureAwait(false);
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
