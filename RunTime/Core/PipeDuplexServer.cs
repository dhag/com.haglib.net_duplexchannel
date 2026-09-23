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

        /// <summary>
        /// これまでに受理した接続の総数。id の通し番号（P0001…）と同じ。
        /// 受理されなかった接続との区別を、ログを読まずに付けるために公開する。
        /// </summary>
        public int AcceptedCount => _clientIdCounter;

        /// <summary>
        /// 診断ログの出力先。既定は null で何も出力しない。
        /// このファイルは SimpleMcpServer.csproj に Link されており
        /// UnityEngine に依存できないため、ホスト側で購読すること。
        /// </summary>
        public static Action<string> DiagnosticLog;

        private static void Diag(string message)
        {
            var sink = DiagnosticLog;
            if (sink == null) return;
            try { sink(message); } catch { }
        }

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

            // _cts.Dispose() は呼ばない。
            //
            // AcceptLoopAsync が待っている WaitForConnectionAsync は、
            // キャンセル後に IO 完了スレッドからコールバックが走り、
            // CancellationTokenRegistration が _cts.Token を読む。
            // ここで破棄すると ObjectDisposedException になり、
            // native-to-managed のコールバック上で未処理例外になるため
            // Unity ごと落ちる（Editor.log に RegisterUECatcher → Shut down. として残る）。
            // Cancel 済みなので登録は解除され、実体はファイナライザで回収される。
        }

        /// <summary>
        /// 接続を受け付け続ける。
        ///
        /// 【待ち受けを切らさないこと】
        ///   NamedPipeServerStream は maxNumberOfServerInstances=16 で作れるが、
        ///   実際に待ち受けているのは「WaitForConnectionAsync を呼んでいるインスタンス」
        ///   だけである。受理してからチャネルを組み立てるまでの間に次のリスナを
        ///   作らないと、その隙間に来たクライアントは
        ///   「接続は成功するのに対応するサーバインスタンスが無い」状態になり、
        ///   要求が誰にも読まれず応答も返らない。
        ///   実測では 3 回に 2 回がこの状態に落ちていた。
        ///
        ///   そのため、受理したら真っ先に次のリスナを作り、チャネルの生成は
        ///   そのあとに行う。
        /// </summary>
        private async Task AcceptLoopAsync(CancellationToken ct)
        {
            NamedPipeServerStream pipeServer = null;

            try
            {
                while (!ct.IsCancellationRequested && _clients.Count < _maxClients)
                {
                    if (pipeServer == null)
                    {
                        pipeServer = CreateListener();
                        Diag($"[PipeDuplexServer] リスナ生成: pipe={_pipeName}; 接続数={_clients.Count}");
                    }

                    NamedPipeServerStream accepted;
                    try
                    {
                        // クライアント接続を待つ
                        await pipeServer.WaitForConnectionAsync(ct).ConfigureAwait(false);

                        // 受理したものをローカルへ移し、待ち受け枠を空ける。
                        accepted   = pipeServer;
                        pipeServer = null;

                        if (ct.IsCancellationRequested)
                        {
                            accepted.Close();
                            Diag("[PipeDuplexServer] accept ループ中断: 受理直後にキャンセル");
                            break;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        pipeServer?.Close();
                        pipeServer = null;
                        Diag("[PipeDuplexServer] accept ループ中断: キャンセル");
                        break;
                    }
                    catch (Exception ex)
                    {
                        pipeServer?.Close();
                        pipeServer = null;
                        Diag($"[PipeDuplexServer] 接続待ちで例外: {ex.GetType().Name}: {ex.Message}");
                        continue;   // 次の接続を待つ
                    }

                    var clientId = $"P{Interlocked.Increment(ref _clientIdCounter):D4}";
                    Diag($"[PipeDuplexServer] 接続受理: id={clientId}");

                    // ★ チャネルを組み立てる前に次のリスナを立てる。
                    //    ここを後回しにすると待ち受けが切れる。
                    if (!ct.IsCancellationRequested && _clients.Count + 1 < _maxClients)
                    {
                        try
                        {
                            pipeServer = CreateListener();
                            Diag($"[PipeDuplexServer] リスナ生成: pipe={_pipeName}; 接続数={_clients.Count}");
                        }
                        catch (Exception ex)
                        {
                            pipeServer = null;
                            Diag($"[PipeDuplexServer] リスナ生成に失敗: {ex.GetType().Name}: {ex.Message}");
                        }
                    }

                    try
                    {
                        var channel = new PipeDuplexChannel(accepted, clientId);

                        // イベントハンドラ設定
                        channel.OnReceived += (ch, msg) => OnReceived?.Invoke(ch, msg);
                        channel.OnDisconnected += HandleClientDisconnected;

                        _clients[clientId] = channel;

                        // 受信ループ開始
                        channel.StartReceiving();

                        OnClientConnected?.Invoke(channel);
                    }
                    catch (Exception ex)
                    {
                        try { accepted.Close(); } catch { }
                        Diag($"[PipeDuplexServer] チャネル生成で例外: id={clientId}; {ex.GetType().Name}: {ex.Message}");
                    }
                }

                Diag($"[PipeDuplexServer] accept ループ終了: 接続数={_clients.Count}; キャンセル={ct.IsCancellationRequested}");
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                Diag($"[PipeDuplexServer] accept ループが例外で終了: {ex.GetType().Name}: {ex.Message}");
            }
            finally
            {
                try { pipeServer?.Close(); } catch { }
            }
        }

        /// <summary>待ち受け用の NamedPipeServerStream を 1 個作る。</summary>
        private NamedPipeServerStream CreateListener()
            => new NamedPipeServerStream(
                _pipeName,
                PipeDirection.InOut,
                _maxClients,
                PipeTransmissionMode.Byte,
                PipeOptions.Asynchronous);

        private void HandleClientDisconnected(IDuplexChannel channel)
        {
            if (_clients.TryRemove(channel.Id, out var removed))
            {
                OnClientDisconnected?.Invoke(channel);

                // ReceiveLoopAsync の finally は ClosePipe を呼ばないため、
                // ここで破棄しないと NamedPipeServerStream が開いたまま残る。
                try
                {
                    removed?.Dispose();
                    Diag($"[PipeDuplexServer] 切断処理: id={channel.Id}; 破棄=成功; 接続数={_clients.Count}");
                }
                catch (Exception ex)
                {
                    Diag($"[PipeDuplexServer] 切断処理: id={channel.Id}; 破棄=例外 {ex.GetType().Name}: {ex.Message}");
                }
            }
        }
    }
}
