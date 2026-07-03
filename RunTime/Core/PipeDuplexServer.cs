using System;
using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// 名前付きパイプ双方向通信サーバー
    /// </summary>
    public class PipeDuplexServer : IDuplexServer
    {
        private readonly string _pipeName;
        private readonly ConcurrentDictionary<string, PipeDuplexChannel> _clients;
        private readonly CancellationTokenSource _cts;
        private Task _acceptTask;
        private bool _disposed;
        private bool _isListening;
        private int _clientIdCounter;
        private readonly int _maxClients;

        public bool IsListening => _isListening;

        public IDuplexChannel[] Clients
        {
            get
            {
                var channels = new IDuplexChannel[_clients.Count];
                var i = 0;
                foreach (var client in _clients.Values)
                {
                    channels[i++] = client;
                }
                return channels;
            }
        }

        public event Action<IDuplexChannel> OnClientConnected;
        public event Action<IDuplexChannel> OnClientDisconnected;
        public event Action<IDuplexChannel, DuplexMessage> OnReceived;

        /// <summary>
        /// コンストラクタ
        /// </summary>
        /// <param name="pipeName">パイプ名</param>
        /// <param name="maxClients">最大クライアント数</param>
        public PipeDuplexServer(string pipeName, int maxClients = 16)
        {
            _pipeName = pipeName ?? throw new ArgumentNullException(nameof(pipeName));
            _maxClients = maxClients;
            _clients = new ConcurrentDictionary<string, PipeDuplexChannel>();
            _cts = new CancellationTokenSource();
        }

        /// <summary>
        /// リッスン開始
        /// </summary>
        public Task StartAsync(int port, CancellationToken ct = default)
        {
            // port は無視（パイプ名で識別）
            return StartAsync(ct);
        }

        /// <summary>
        /// リッスン開始
        /// </summary>
        public Task StartAsync(CancellationToken ct = default)
        {
            if (_isListening)
                throw new InvalidOperationException("Server is already running.");

            _isListening = true;
            _acceptTask = Task.Run(() => AcceptLoopAsync(_cts.Token), ct);

            return Task.CompletedTask;
        }

        /// <summary>
        /// 全クライアントにブロードキャスト
        /// </summary>
        public async Task BroadcastAsync(DuplexMessage message, CancellationToken ct = default)
        {
            var tasks = new Task[_clients.Count];
            var i = 0;
            foreach (var client in _clients.Values)
            {
                tasks[i++] = client.SendAsync(message, ct);
            }

            try
            {
                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch { }
        }

        public Task BroadcastAsync(string text, CancellationToken ct = default)
        {
            return BroadcastAsync(new DuplexMessage(text), ct);
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
        /// サーバー停止
        /// </summary>
        public async Task StopAsync()
        {
            if (_disposed) return;

            _isListening = false;
            _cts.Cancel();

            // 全クライアント切断
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

            _isListening = false;
            _cts.Cancel();

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
                while (!ct.IsCancellationRequested && _clients.Count < _maxClients)
                {
                    // 新しいパイプサーバーインスタンスを作成
                    var pipeServer = new NamedPipeServerStream(
                        _pipeName,
                        PipeDirection.InOut,
                        _maxClients,
                        PipeTransmissionMode.Byte,
                        PipeOptions.Asynchronous);

                    try
                    {
                        // クライアント接続を待つ
                        await pipeServer.WaitForConnectionAsync(ct).ConfigureAwait(false);

                        if (ct.IsCancellationRequested)
                        {
                            pipeServer.Close();
                            break;
                        }

                        var clientId = $"P{Interlocked.Increment(ref _clientIdCounter):D4}";
                        var channel = new PipeDuplexChannel(pipeServer, clientId);

                        // イベントハンドラ設定
                        channel.OnReceived += (ch, msg) => OnReceived?.Invoke(ch, msg);
                        channel.OnDisconnected += HandleClientDisconnected;

                        _clients[clientId] = channel;

                        // 受信ループ開始
                        channel.StartReceiving();

                        OnClientConnected?.Invoke(channel);
                    }
                    catch (OperationCanceledException)
                    {
                        pipeServer.Close();
                        break;
                    }
                    catch (Exception)
                    {
                        pipeServer.Close();
                        // 次の接続を待つ
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception) { }
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
