using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// パース済みアイテム
    /// </summary>
    public class ParsedItem<T>
    {
        /// <summary>送信元チャネル</summary>
        public IDuplexChannel Channel { get; set; }

        /// <summary>元メッセージ（応答用）</summary>
        public DuplexMessage OriginalMessage { get; set; }

        /// <summary>パース済みデータ</summary>
        public T Data { get; set; }

        /// <summary>受信時刻</summary>
        public DateTime ReceivedAt { get; set; }

        /// <summary>パース完了時刻</summary>
        public DateTime ParsedAt { get; set; }

        /// <summary>リクエストか</summary>
        public bool IsRequest => OriginalMessage?.Type == MessageType.Request;

        /// <summary>リクエストに応答</summary>
        public Task ReplyAsync(DuplexMessage response, CancellationToken ct = default)
            => Channel.ReplyAsync(OriginalMessage, response, ct);

        /// <summary>リクエストに応答（文字列）</summary>
        public Task ReplyAsync(string text, CancellationToken ct = default)
            => Channel.ReplyAsync(OriginalMessage, text, ct);

        /// <summary>リクエストに応答（TypedPayload）</summary>
        public Task ReplyAsync(TypedPayload payload, CancellationToken ct = default)
            => Channel.ReplyAsync(OriginalMessage, payload, ct);
    }

    /// <summary>
    /// 生メッセージ（内部用）
    /// </summary>
    internal class RawQueueItem
    {
        public IDuplexChannel Channel { get; set; }
        public DuplexMessage Message { get; set; }
        public DateTime ReceivedAt { get; set; }
    }

    /// <summary>
    /// パース処理を別スレッドで行うメッセージキュー
    /// 
    /// 受信スレッド → 生データキュー → ワーカースレッド → パース済みキュー → メインスレッド
    ///                 (軽い)            (重い処理)          (結果だけ取得)
    /// </summary>
    public class ParsedMessageQueue<T> : IDisposable
    {
        // 生データキュー（受信スレッドが入れる）
        private readonly ConcurrentQueue<RawQueueItem> _rawPushQueue = new();
        private readonly ConcurrentQueue<RawQueueItem> _rawRequestQueue = new();
        private readonly SemaphoreSlim _rawPushSignal = new(0);
        private readonly SemaphoreSlim _rawRequestSignal = new(0);

        // パース済みキュー（メインスレッドが読む）
        private readonly ConcurrentQueue<ParsedItem<T>> _parsedPushQueue = new();
        private readonly ConcurrentQueue<ParsedItem<T>> _parsedRequestQueue = new();
        private readonly SemaphoreSlim _parsedPushSignal = new(0);
        private readonly SemaphoreSlim _parsedRequestSignal = new(0);
        private readonly SemaphoreSlim _parsedAnySignal = new(0);

        private readonly Func<DuplexMessage, T> _parser;
        private readonly CancellationTokenSource _cts = new();
        private readonly Task[] _pushWorkers;
        private readonly Task[] _requestWorkers;
        private bool _disposed;

        /// <summary>パース済み Push キュー数</summary>
        public int ParsedPushCount => _parsedPushQueue.Count;

        /// <summary>パース済み Request キュー数</summary>
        public int ParsedRequestCount => _parsedRequestQueue.Count;

        /// <summary>未処理の生 Push キュー数</summary>
        public int RawPushCount => _rawPushQueue.Count;

        /// <summary>未処理の生 Request キュー数</summary>
        public int RawRequestCount => _rawRequestQueue.Count;

        /// <summary>
        /// コンストラクタ
        /// </summary>
        /// <param name="parser">パース関数（重い処理）</param>
        /// <param name="pushWorkerCount">Push用ワーカー数</param>
        /// <param name="requestWorkerCount">Request用ワーカー数（応答が必要なので優先）</param>
        public ParsedMessageQueue(
            Func<DuplexMessage, T> parser,
            int pushWorkerCount = 1,
            int requestWorkerCount = 1)
        {
            _parser = parser ?? throw new ArgumentNullException(nameof(parser));

            _pushWorkers = new Task[pushWorkerCount];
            for (int i = 0; i < pushWorkerCount; i++)
            {
                var workerId = i;
                _pushWorkers[i] = Task.Run(() => PushWorkerLoop(workerId, _cts.Token));
            }

            _requestWorkers = new Task[requestWorkerCount];
            for (int i = 0; i < requestWorkerCount; i++)
            {
                var workerId = i;
                _requestWorkers[i] = Task.Run(() => RequestWorkerLoop(workerId, _cts.Token));
            }
        }

        #region エンキュー（受信スレッドから呼ぶ、軽い）

        /// <summary>
        /// 受信スレッドからメッセージをエンキュー（軽い処理）
        /// </summary>
        public void EnqueueRaw(IDuplexChannel channel, DuplexMessage message)
        {
            if (_disposed) return;

            var item = new RawQueueItem
            {
                Channel = channel,
                Message = message,
                ReceivedAt = DateTime.UtcNow
            };

            if (message.Type == MessageType.Request)
            {
                _rawRequestQueue.Enqueue(item);
                _rawRequestSignal.Release();
            }
            else
            {
                _rawPushQueue.Enqueue(item);
                _rawPushSignal.Release();
            }
        }

        #endregion

        #region ワーカースレッド

        private async Task PushWorkerLoop(int workerId, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await _rawPushSignal.WaitAsync(ct).ConfigureAwait(false);

                    if (!_rawPushQueue.TryDequeue(out var raw))
                        continue;

                    var parsed = ParseItem(raw);
                    if (parsed != null)
                    {
                        _parsedPushQueue.Enqueue(parsed);
                        _parsedPushSignal.Release();
                        _parsedAnySignal.Release();
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"PushWorker[{workerId}] error: {ex.Message}");
                }
            }
        }

        private async Task RequestWorkerLoop(int workerId, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await _rawRequestSignal.WaitAsync(ct).ConfigureAwait(false);

                    if (!_rawRequestQueue.TryDequeue(out var raw))
                        continue;

                    var parsed = ParseItem(raw);
                    if (parsed != null)
                    {
                        _parsedRequestQueue.Enqueue(parsed);
                        _parsedRequestSignal.Release();
                        _parsedAnySignal.Release();
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"RequestWorker[{workerId}] error: {ex.Message}");
                }
            }
        }

        private ParsedItem<T> ParseItem(RawQueueItem raw)
        {
            try
            {
                var data = _parser(raw.Message);

                return new ParsedItem<T>
                {
                    Channel = raw.Channel,
                    OriginalMessage = raw.Message,
                    Data = data,
                    ReceivedAt = raw.ReceivedAt,
                    ParsedAt = DateTime.UtcNow
                };
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Parse error: {ex.Message}");
                return null;
            }
        }

        #endregion

        #region デキュー（メインスレッドから呼ぶ）

        /// <summary>
        /// パース済み Push を非ブロッキングで取得
        /// </summary>
        public bool TryDequeuePush(out ParsedItem<T> item)
        {
            if (_parsedPushQueue.TryDequeue(out item))
            {
                _parsedPushSignal.Wait(0);
                _parsedAnySignal.Wait(0);
                return true;
            }
            return false;
        }

        /// <summary>
        /// パース済み Request を非ブロッキングで取得
        /// </summary>
        public bool TryDequeueRequest(out ParsedItem<T> item)
        {
            if (_parsedRequestQueue.TryDequeue(out item))
            {
                _parsedRequestSignal.Wait(0);
                _parsedAnySignal.Wait(0);
                return true;
            }
            return false;
        }

        /// <summary>
        /// パース済みメッセージを非ブロッキングで取得（Request優先）
        /// </summary>
        public bool TryDequeue(out ParsedItem<T> item)
        {
            if (TryDequeueRequest(out item))
                return true;
            return TryDequeuePush(out item);
        }

        /// <summary>
        /// 全 Push を取り出す（Unity Update向け）
        /// </summary>
        public int DequeueAllPush(List<ParsedItem<T>> results)
        {
            int count = 0;
            while (TryDequeuePush(out var item))
            {
                results.Add(item);
                count++;
            }
            return count;
        }

        /// <summary>
        /// 全 Request を取り出す（Unity Update向け）
        /// </summary>
        public int DequeueAllRequest(List<ParsedItem<T>> results)
        {
            int count = 0;
            while (TryDequeueRequest(out var item))
            {
                results.Add(item);
                count++;
            }
            return count;
        }

        /// <summary>
        /// 全メッセージを取り出す（Request優先）
        /// </summary>
        public int DequeueAll(List<ParsedItem<T>> results)
        {
            int count = 0;
            count += DequeueAllRequest(results);
            count += DequeueAllPush(results);
            return count;
        }

        #endregion

        #region 非同期デキュー（非Unityスレッド向け）

        /// <summary>
        /// パース済み Push を非同期で待機して取得
        /// </summary>
        public async Task<ParsedItem<T>> DequeuePushAsync(CancellationToken ct = default)
        {
            await _parsedPushSignal.WaitAsync(ct).ConfigureAwait(false);
            _parsedPushQueue.TryDequeue(out var item);
            _parsedAnySignal.Wait(0);
            return item;
        }

        /// <summary>
        /// パース済み Request を非同期で待機して取得
        /// </summary>
        public async Task<ParsedItem<T>> DequeueRequestAsync(CancellationToken ct = default)
        {
            await _parsedRequestSignal.WaitAsync(ct).ConfigureAwait(false);
            _parsedRequestQueue.TryDequeue(out var item);
            _parsedAnySignal.Wait(0);
            return item;
        }

        /// <summary>
        /// いずれかを非同期で待機して取得
        /// </summary>
        public async Task<ParsedItem<T>> DequeueAsync(CancellationToken ct = default)
        {
            await _parsedAnySignal.WaitAsync(ct).ConfigureAwait(false);

            if (_parsedRequestQueue.TryDequeue(out var reqItem))
            {
                _parsedRequestSignal.Wait(0);
                return reqItem;
            }

            if (_parsedPushQueue.TryDequeue(out var pushItem))
            {
                _parsedPushSignal.Wait(0);
                return pushItem;
            }

            return null;
        }

        /// <summary>
        /// タイムアウト付きデキュー
        /// </summary>
        public async Task<ParsedItem<T>> DequeueAsync(int timeoutMs, CancellationToken ct = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeoutMs);
            try
            {
                return await DequeueAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return null;
            }
        }

        #endregion

        #region バインド

        /// <summary>
        /// チャネルの受信をこのキューにバインド
        /// </summary>
        public void BindChannel(IDuplexChannel channel)
        {
            channel.OnReceived += (ch, msg) => EnqueueRaw(ch, msg);
        }

        /// <summary>
        /// サーバーの受信をこのキューにバインド
        /// </summary>
        public void BindServer(IDuplexServer server)
        {
            if (server is TcpDuplexServer tcpServer)
            {
                tcpServer.OnReceived += (ch, msg) => EnqueueRaw(ch, msg);
            }
            else if (server is WebSocketDuplexServer wsServer)
            {
                wsServer.OnReceived += (ch, msg) => EnqueueRaw(ch, msg);
            }
            else if (server is PipeDuplexServer pipeServer)
            {
                pipeServer.OnReceived += (ch, msg) => EnqueueRaw(ch, msg);
            }
        }

        #endregion

        public async Task StopAsync()
        {
            _cts.Cancel();
            await Task.WhenAll(_pushWorkers).ConfigureAwait(false);
            await Task.WhenAll(_requestWorkers).ConfigureAwait(false);
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _cts.Cancel();

            _rawPushSignal?.Dispose();
            _rawRequestSignal?.Dispose();
            _parsedPushSignal?.Dispose();
            _parsedRequestSignal?.Dispose();
            _parsedAnySignal?.Dispose();
            _cts?.Dispose();
        }
    }

    /// <summary>
    /// 拡張メソッド
    /// </summary>
    public static class ParsedMessageQueueExtensions
    {
        /// <summary>
        /// チャネルにパース済みキューを作成してバインド
        /// </summary>
        public static ParsedMessageQueue<T> CreateParsedQueue<T>(
            this IDuplexChannel channel,
            Func<DuplexMessage, T> parser,
            int pushWorkerCount = 1,
            int requestWorkerCount = 1)
        {
            var queue = new ParsedMessageQueue<T>(parser, pushWorkerCount, requestWorkerCount);
            queue.BindChannel(channel);
            return queue;
        }

        /// <summary>
        /// サーバーにパース済みキューを作成してバインド
        /// </summary>
        public static ParsedMessageQueue<T> CreateParsedQueue<T>(
            this IDuplexServer server,
            Func<DuplexMessage, T> parser,
            int pushWorkerCount = 1,
            int requestWorkerCount = 1)
        {
            var queue = new ParsedMessageQueue<T>(parser, pushWorkerCount, requestWorkerCount);
            queue.BindServer(server);
            return queue;
        }
    }
}
