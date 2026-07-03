using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// キュー登録されたメッセージ（チャネル情報付き）
    /// </summary>
    public class QueuedMessage
    {
        /// <summary>送信元チャネル</summary>
        public IDuplexChannel Channel { get; set; }

        /// <summary>メッセージ本体</summary>
        public DuplexMessage Message { get; set; }

        /// <summary>受信時刻</summary>
        public DateTime ReceivedAt { get; set; }

        public QueuedMessage(IDuplexChannel channel, DuplexMessage message)
        {
            Channel = channel;
            Message = message;
            ReceivedAt = DateTime.UtcNow;
        }

        /// <summary>リクエストに応答（便利メソッド）</summary>
        public Task ReplyAsync(DuplexMessage response, CancellationToken ct = default)
        {
            return Channel.ReplyAsync(Message, response, ct);
        }

        /// <summary>リクエストに応答（文字列）</summary>
        public Task ReplyAsync(string text, CancellationToken ct = default)
        {
            return Channel.ReplyAsync(Message, text, ct);
        }

        /// <summary>リクエストに応答（TypedPayload）</summary>
        public Task ReplyAsync(TypedPayload payload, CancellationToken ct = default)
        {
            return Channel.ReplyAsync(Message, payload, ct);
        }
    }

    /// <summary>
    /// 双方向通信メッセージキュー
    /// 
    /// イベントベースの OnReceived の代わりに、キューベースでメッセージを処理
    /// - Push用キューとRequest用キューを分離
    /// - 非同期待機可能な Dequeue
    /// - 複数のチャネルからのメッセージを統合管理可能
    /// </summary>
    public class DuplexMessageQueue : IDisposable
    {
        private readonly ConcurrentQueue<QueuedMessage> _pushQueue;
        private readonly ConcurrentQueue<QueuedMessage> _requestQueue;
        private readonly SemaphoreSlim _pushSignal;
        private readonly SemaphoreSlim _requestSignal;
        private readonly SemaphoreSlim _anySignal;  // どちらかに来たら通知
        private bool _disposed;

        /// <summary>Push キュー内のメッセージ数</summary>
        public int PushCount => _pushQueue.Count;

        /// <summary>Request キュー内のメッセージ数</summary>
        public int RequestCount => _requestQueue.Count;

        /// <summary>全キュー内のメッセージ数</summary>
        public int TotalCount => _pushQueue.Count + _requestQueue.Count;

        public DuplexMessageQueue()
        {
            _pushQueue = new ConcurrentQueue<QueuedMessage>();
            _requestQueue = new ConcurrentQueue<QueuedMessage>();
            _pushSignal = new SemaphoreSlim(0);
            _requestSignal = new SemaphoreSlim(0);
            _anySignal = new SemaphoreSlim(0);
        }

        #region エンキュー（内部用、チャネルから呼ばれる）

        /// <summary>
        /// メッセージをエンキューして通知
        /// チャネルの HandleMessage から呼ばれる想定
        /// </summary>
        public void Enqueue(IDuplexChannel channel, DuplexMessage message)
        {
            if (_disposed) return;

            var queued = new QueuedMessage(channel, message);

            if (message.Type == MessageType.Request)
            {
                _requestQueue.Enqueue(queued);
                _requestSignal.Release();
            }
            else // Push
            {
                _pushQueue.Enqueue(queued);
                _pushSignal.Release();
            }

            // どちらかに来た通知
            _anySignal.Release();
        }

        #endregion

        #region Push キュー操作

        /// <summary>
        /// Push メッセージを非同期で待機して取得
        /// </summary>
        public async Task<QueuedMessage> DequeuePushAsync(CancellationToken ct = default)
        {
            await _pushSignal.WaitAsync(ct).ConfigureAwait(false);
            
            if (_pushQueue.TryDequeue(out var message))
            {
                return message;
            }

            // シグナルがあったのに取れない場合（通常は起きない）
            throw new InvalidOperationException("Push queue signal fired but no message available.");
        }

        /// <summary>
        /// Push メッセージを非ブロッキングで取得
        /// </summary>
        public bool TryDequeuePush(out QueuedMessage message)
        {
            if (_pushQueue.TryDequeue(out message))
            {
                // シグナルも減らす（同期を保つ）
                _pushSignal.Wait(0);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Push メッセージを覗き見（取り出さない）
        /// </summary>
        public bool TryPeekPush(out QueuedMessage message)
        {
            return _pushQueue.TryPeek(out message);
        }

        #endregion

        #region Request キュー操作

        /// <summary>
        /// Request メッセージを非同期で待機して取得
        /// </summary>
        public async Task<QueuedMessage> DequeueRequestAsync(CancellationToken ct = default)
        {
            await _requestSignal.WaitAsync(ct).ConfigureAwait(false);
            
            if (_requestQueue.TryDequeue(out var message))
            {
                return message;
            }

            throw new InvalidOperationException("Request queue signal fired but no message available.");
        }

        /// <summary>
        /// Request メッセージを非ブロッキングで取得
        /// </summary>
        public bool TryDequeueRequest(out QueuedMessage message)
        {
            if (_requestQueue.TryDequeue(out message))
            {
                _requestSignal.Wait(0);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Request メッセージを覗き見（取り出さない）
        /// </summary>
        public bool TryPeekRequest(out QueuedMessage message)
        {
            return _requestQueue.TryPeek(out message);
        }

        #endregion

        #region 共通操作（どちらかのキュー）

        /// <summary>
        /// いずれかのキューにメッセージが来るまで待機
        /// </summary>
        public async Task WaitAnyAsync(CancellationToken ct = default)
        {
            await _anySignal.WaitAsync(ct).ConfigureAwait(false);
        }

        /// <summary>
        /// いずれかのキューからメッセージを取得（Request優先）
        /// </summary>
        public async Task<QueuedMessage> DequeueAnyAsync(CancellationToken ct = default)
        {
            await _anySignal.WaitAsync(ct).ConfigureAwait(false);

            // Request を優先
            if (_requestQueue.TryDequeue(out var reqMsg))
            {
                _requestSignal.Wait(0);
                return reqMsg;
            }

            if (_pushQueue.TryDequeue(out var pushMsg))
            {
                _pushSignal.Wait(0);
                return pushMsg;
            }

            throw new InvalidOperationException("Any signal fired but no message available.");
        }

        /// <summary>
        /// いずれかのキューから非ブロッキングで取得（Request優先）
        /// </summary>
        public bool TryDequeueAny(out QueuedMessage message)
        {
            if (TryDequeueRequest(out message))
            {
                _anySignal.Wait(0);
                return true;
            }

            if (TryDequeuePush(out message))
            {
                _anySignal.Wait(0);
                return true;
            }

            return false;
        }

        #endregion

        #region タイムアウト付きデキュー

        /// <summary>
        /// Push メッセージをタイムアウト付きで取得
        /// </summary>
        public async Task<QueuedMessage> DequeuePushAsync(int timeoutMs, CancellationToken ct = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeoutMs);
            
            try
            {
                return await DequeuePushAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return null; // タイムアウト
            }
        }

        /// <summary>
        /// Request メッセージをタイムアウト付きで取得
        /// </summary>
        public async Task<QueuedMessage> DequeueRequestAsync(int timeoutMs, CancellationToken ct = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeoutMs);
            
            try
            {
                return await DequeueRequestAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return null; // タイムアウト
            }
        }

        /// <summary>
        /// いずれかのメッセージをタイムアウト付きで取得
        /// </summary>
        public async Task<QueuedMessage> DequeueAnyAsync(int timeoutMs, CancellationToken ct = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeoutMs);
            
            try
            {
                return await DequeueAnyAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return null; // タイムアウト
            }
        }

        #endregion

        #region キュークリア

        /// <summary>
        /// Push キューをクリア
        /// </summary>
        public void ClearPush()
        {
            while (_pushQueue.TryDequeue(out _))
            {
                _pushSignal.Wait(0);
                _anySignal.Wait(0);
            }
        }

        /// <summary>
        /// Request キューをクリア
        /// </summary>
        public void ClearRequest()
        {
            while (_requestQueue.TryDequeue(out _))
            {
                _requestSignal.Wait(0);
                _anySignal.Wait(0);
            }
        }

        /// <summary>
        /// 全キューをクリア
        /// </summary>
        public void ClearAll()
        {
            ClearPush();
            ClearRequest();
        }

        #endregion

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _pushSignal?.Dispose();
            _requestSignal?.Dispose();
            _anySignal?.Dispose();
        }
    }

    /// <summary>
    /// IDuplexChannel / IDuplexServer をキューにバインドする拡張メソッド
    /// </summary>
    public static class DuplexQueueExtensions
    {
        /// <summary>
        /// チャネルの受信をキューにバインド
        /// チャネルの OnReceived イベントをキューへのエンキューに接続
        /// </summary>
        public static void BindToQueue(this IDuplexChannel channel, DuplexMessageQueue queue)
        {
            channel.OnReceived += (ch, msg) => queue.Enqueue(ch, msg);
        }

        /// <summary>
        /// サーバーの全クライアント受信をキューにバインド
        /// </summary>
        public static void BindToQueue(this IDuplexServer server, DuplexMessageQueue queue)
        {
            // サーバーの OnReceived があれば使う（TcpDuplexServer, WebSocketDuplexServer など）
            if (server is TcpDuplexServer tcpServer)
            {
                tcpServer.OnReceived += (ch, msg) => queue.Enqueue(ch, msg);
            }
            else if (server is WebSocketDuplexServer wsServer)
            {
                wsServer.OnReceived += (ch, msg) => queue.Enqueue(ch, msg);
            }
            else if (server is PipeDuplexServer pipeServer)
            {
                pipeServer.OnReceived += (ch, msg) => queue.Enqueue(ch, msg);
            }
            // 他のサーバータイプにも対応可能
        }

        /// <summary>
        /// キューを作成してチャネルにバインド（便利メソッド）
        /// </summary>
        public static DuplexMessageQueue CreateBoundQueue(this IDuplexChannel channel)
        {
            var queue = new DuplexMessageQueue();
            channel.BindToQueue(queue);
            return queue;
        }

        /// <summary>
        /// キューを作成してサーバーにバインド（便利メソッド）
        /// </summary>
        public static DuplexMessageQueue CreateBoundQueue(this IDuplexServer server)
        {
            var queue = new DuplexMessageQueue();
            server.BindToQueue(queue);
            return queue;
        }
    }
}
