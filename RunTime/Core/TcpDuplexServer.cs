using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// TCP双方向通信サーバー
    /// </summary>
    public class TcpDuplexServer : IDuplexServer
    {
        private Socket _listener;
        private readonly ConcurrentDictionary<string, TcpDuplexChannel> _clients;
        private readonly CancellationTokenSource _cts;
        private Task _acceptTask;
        private bool _disposed;
        private int _clientIdCounter;

        public bool IsListening => _listener?.IsBound ?? false;

        /// <summary>
        /// これまでに受理した接続の総数。id の通し番号（C0001…）と同じ。
        /// </summary>
        public int AcceptedCount => _clientIdCounter;

        /// <summary>
        /// 診断ログの出力先。既定は null で何も出力しない。
        /// このファイルは SimpleMcpServer.csproj に Link され得るため
        /// UnityEngine に依存できない。ホスト側で購読すること。
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

        /// <summary>
        /// メッセージ受信イベント（全クライアント共通ハンドラ）
        /// </summary>
        public event Action<IDuplexChannel, DuplexMessage> OnReceived;

        public TcpDuplexServer()
        {
            _clients = new ConcurrentDictionary<string, TcpDuplexChannel>();
            _cts = new CancellationTokenSource();
        }

        /// <summary>
        /// リッスン開始
        /// </summary>
        public Task StartAsync(int port, CancellationToken ct = default)
        {
            return StartAsync(IPAddress.Any, port, ct);
        }

        /// <summary>
        /// リッスン開始（アドレス指定）
        /// </summary>
        public Task StartAsync(IPAddress address, int port, CancellationToken ct = default)
        {
            if (_listener != null)
                throw new InvalidOperationException("Server is already running.");

            _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            // 前の待ち受けで受理した接続が、まだ同じポートを使っていても bind できるようにする。
            // Editor 制御（8770）では、要求の途中の接続を抱えたままドメインリロードに入ると、
            // 前の待ち受けを閉じたあとも受理済みの接続が残り、再起動の bind が
            // 「各ソケット アドレスに対して…1 つのみ」で失敗していた（2026-10-04、2 回再現）。
            try { _listener.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true); }
            catch { }

            try
            {
                _listener.Bind(new IPEndPoint(address, port));
                _listener.Listen(100);
            }
            catch
            {
                // bind できなかったソケットは呼び出し側へ残さない（次の StartAsync を通すため）。
                try { _listener.Close(); } catch { }
                _listener = null;
                throw;
            }

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
        /// 特定のタグを持つクライアントにのみ送信
        /// </summary>
        public async Task SendToTagAsync(string tag, DuplexMessage message, CancellationToken ct = default)
        {
            foreach (var client in _clients.Values)
            {
                // TODO: クライアントにタグを持たせる場合はここでフィルタ
                await client.SendAsync(message, ct).ConfigureAwait(false);
            }
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

            _cts.Cancel();

            // リスナー停止
            try
            {
                _listener?.Close();
            }
            catch { }

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

            // Accept ループ終了待ち
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

            try { _listener?.Close(); } catch { }

            // 受理済みの接続を先に閉じる。待ち受けを閉じても、受理済みの接続は
            // 同じポートを使い続けるので、残っていると再起動の bind を妨げる。
            // accept ループの終了を待つより前に閉じる（待ちが時間切れでも閉じ漏れないように）。
            foreach (var client in _clients.Values)
            {
                try { client.Dispose(); } catch { }
            }
            _clients.Clear();

            // accept ループの終了を待つ。
            // 待ち受け中の AcceptAsync が残ったままドメインリロードに入ると、
            // Close したはずの待ち受けソケットが OS 側で開いたまま残り、
            // リロード後の再起動で同じポートに bind できなくなる（Editor 制御 8770 で発生）。
            // ループは ConfigureAwait(false) でスレッドプール上を回るので、ここで待っても詰まらない。
            try { _acceptTask?.Wait(2000); } catch { }

            // 待っている間に受理されたものがあれば、それも閉じる（受理側は停止中なら自分で閉じるが念のため）。
            foreach (var client in _clients.Values)
            {
                try { client.Dispose(); } catch { }
            }
            _clients.Clear();

            // _cts.Dispose() は呼ばない。AcceptAsync の完了コールバックが
            // 後から Token を読むため、破棄すると未処理例外になる
            // （PipeDuplexServer.Dispose で同じ構造の落ち方を確認済み）。
        }

        private async Task AcceptLoopAsync(CancellationToken ct)
        {
            Diag("[TcpDuplexServer] accept ループ開始");

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var socket = await _listener.AcceptAsync().ConfigureAwait(false);

                    // 停止の途中で受理が完了したものは、登録せずに閉じる
                    // （登録すると停止側の閉じ漏れになり、ポートを掴んだまま残る）。
                    if (ct.IsCancellationRequested || _disposed)
                    {
                        try { socket.Close(); } catch { }
                        break;
                    }

                    var clientId = $"C{Interlocked.Increment(ref _clientIdCounter):D4}";
                    Diag($"[TcpDuplexServer] 接続受理: id={clientId}; 接続数={_clients.Count}");

                    var channel = new TcpDuplexChannel(socket, clientId);

                    // イベントハンドラ設定
                    channel.OnReceived += (ch, msg) => OnReceived?.Invoke(ch, msg);
                    channel.OnDisconnected += HandleClientDisconnected;

                    _clients[clientId] = channel;

                    // 受信ループ開始
                    channel.StartReceiving();

                    OnClientConnected?.Invoke(channel);
                }
            }
            catch (OperationCanceledException) { }
            catch (SocketException ex) { Diag($"[TcpDuplexServer] accept 終了: SocketException: {ex.Message}"); }
            catch (ObjectDisposedException) { }
            catch (Exception ex) { Diag($"[TcpDuplexServer] accept ループが例外で終了: {ex.GetType().Name}: {ex.Message}"); }
            finally { Diag("[TcpDuplexServer] accept ループ終了"); }
        }

        private void HandleClientDisconnected(IDuplexChannel channel)
        {
            if (_clients.TryRemove(channel.Id, out var removed))
            {
                OnClientDisconnected?.Invoke(channel);

                try
                {
                    removed?.Dispose();
                    Diag($"[TcpDuplexServer] 切断処理: id={channel.Id}; 接続数={_clients.Count}");
                }
                catch (Exception ex)
                {
                    Diag($"[TcpDuplexServer] 切断処理で例外: id={channel.Id}; {ex.GetType().Name}: {ex.Message}");
                }
            }
        }
    }
}
