using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// キュー機能を内蔵したチャネル用のインターフェース
    /// </summary>
    public interface IQueuedDuplexChannel : IDuplexChannel
    {
        /// <summary>Push メッセージ待機数</summary>
        int PushQueueCount { get; }

        /// <summary>Request メッセージ待機数</summary>
        int RequestQueueCount { get; }

        /// <summary>Push メッセージを非同期で取得</summary>
        Task<DuplexMessage> DequeuePushAsync(CancellationToken ct = default);

        /// <summary>Request メッセージを非同期で取得</summary>
        Task<DuplexMessage> DequeueRequestAsync(CancellationToken ct = default);

        /// <summary>いずれかのメッセージを非同期で取得</summary>
        Task<DuplexMessage> DequeueAsync(CancellationToken ct = default);

        /// <summary>Push メッセージを非ブロッキングで取得</summary>
        bool TryDequeuePush(out DuplexMessage message);

        /// <summary>Request メッセージを非ブロッキングで取得</summary>
        bool TryDequeueRequest(out DuplexMessage message);
    }

    /// <summary>
    /// キュー機能を提供する共通ヘルパークラス
    /// 各チャネル実装から使用
    /// </summary>
    public class MessageQueueHelper : IDisposable
    {
        private readonly ConcurrentQueue<DuplexMessage> _pushQueue;
        private readonly ConcurrentQueue<DuplexMessage> _requestQueue;
        private readonly SemaphoreSlim _pushSignal;
        private readonly SemaphoreSlim _requestSignal;
        private readonly SemaphoreSlim _anySignal;
        private bool _disposed;

        /// <summary>イベントモードが有効か（キューモードと排他）</summary>
        public bool UseEventMode { get; set; } = true;

        public int PushQueueCount => _pushQueue.Count;
        public int RequestQueueCount => _requestQueue.Count;

        public MessageQueueHelper()
        {
            _pushQueue = new ConcurrentQueue<DuplexMessage>();
            _requestQueue = new ConcurrentQueue<DuplexMessage>();
            _pushSignal = new SemaphoreSlim(0);
            _requestSignal = new SemaphoreSlim(0);
            _anySignal = new SemaphoreSlim(0);
        }

        /// <summary>
        /// メッセージを処理（共通エントリポイント）
        /// キューモードならエンキュー、イベントモードならイベント発火
        /// </summary>
        /// <param name="message">受信メッセージ</param>
        /// <param name="onEvent">イベントモード時のコールバック</param>
        /// <returns>イベントを発火すべきか</returns>
        public bool HandleMessage(DuplexMessage message, out bool shouldFireEvent)
        {
            shouldFireEvent = false;

            if (UseEventMode)
            {
                // イベントモード：従来どおりイベントで通知
                shouldFireEvent = true;
                return false;
            }

            // キューモード：エンキューして通知
            EnqueueInternal(message);
            return true;
        }

        /// <summary>
        /// メッセージをエンキュー（直接呼び出し用）
        /// </summary>
        public void Enqueue(DuplexMessage message)
        {
            EnqueueInternal(message);
        }

        private void EnqueueInternal(DuplexMessage message)
        {
            if (_disposed) return;

            if (message.Type == MessageType.Request)
            {
                _requestQueue.Enqueue(message);
                _requestSignal.Release();
            }
            else
            {
                _pushQueue.Enqueue(message);
                _pushSignal.Release();
            }

            _anySignal.Release();
        }

        #region Dequeue Operations

        public async Task<DuplexMessage> DequeuePushAsync(CancellationToken ct = default)
        {
            await _pushSignal.WaitAsync(ct).ConfigureAwait(false);
            _pushQueue.TryDequeue(out var message);
            return message;
        }

        public async Task<DuplexMessage> DequeueRequestAsync(CancellationToken ct = default)
        {
            await _requestSignal.WaitAsync(ct).ConfigureAwait(false);
            _requestQueue.TryDequeue(out var message);
            return message;
        }

        public async Task<DuplexMessage> DequeueAsync(CancellationToken ct = default)
        {
            await _anySignal.WaitAsync(ct).ConfigureAwait(false);

            // Request 優先
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

            return null;
        }

        public bool TryDequeuePush(out DuplexMessage message)
        {
            if (_pushQueue.TryDequeue(out message))
            {
                _pushSignal.Wait(0);
                _anySignal.Wait(0);
                return true;
            }
            return false;
        }

        public bool TryDequeueRequest(out DuplexMessage message)
        {
            if (_requestQueue.TryDequeue(out message))
            {
                _requestSignal.Wait(0);
                _anySignal.Wait(0);
                return true;
            }
            return false;
        }

        public bool TryDequeue(out DuplexMessage message)
        {
            if (TryDequeueRequest(out message))
                return true;
            return TryDequeuePush(out message);
        }

        #endregion

        #region Timeout versions

        public async Task<DuplexMessage> DequeuePushAsync(int timeoutMs, CancellationToken ct = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeoutMs);
            try
            {
                return await DequeuePushAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return null;
            }
        }

        public async Task<DuplexMessage> DequeueRequestAsync(int timeoutMs, CancellationToken ct = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeoutMs);
            try
            {
                return await DequeueRequestAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                return null;
            }
        }

        public async Task<DuplexMessage> DequeueAsync(int timeoutMs, CancellationToken ct = default)
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

        public void ClearAll()
        {
            while (_pushQueue.TryDequeue(out _))
            {
                _pushSignal.Wait(0);
                _anySignal.Wait(0);
            }

            while (_requestQueue.TryDequeue(out _))
            {
                _requestSignal.Wait(0);
                _anySignal.Wait(0);
            }
        }

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
    /// キュー機能を内蔵した TcpDuplexChannel の例
    /// 既存の TcpDuplexChannel を継承してキュー機能を追加
    /// </summary>
    public class QueuedTcpDuplexChannel : TcpDuplexChannel, IQueuedDuplexChannel
    {
        private readonly MessageQueueHelper _queueHelper;

        public int PushQueueCount => _queueHelper.PushQueueCount;
        public int RequestQueueCount => _queueHelper.RequestQueueCount;

        /// <summary>
        /// キューモードを有効にする（デフォルトはイベントモード）
        /// </summary>
        public bool UseQueueMode
        {
            get => !_queueHelper.UseEventMode;
            set => _queueHelper.UseEventMode = !value;
        }

        public QueuedTcpDuplexChannel() : base()
        {
            _queueHelper = new MessageQueueHelper();
            // 内部の OnReceived をフックしてキューに流す
            base.OnReceived += (ch, msg) =>
            {
                if (!_queueHelper.UseEventMode)
                {
                    _queueHelper.Enqueue(msg);
                }
            };
        }

        public Task<DuplexMessage> DequeuePushAsync(CancellationToken ct = default)
            => _queueHelper.DequeuePushAsync(ct);

        public Task<DuplexMessage> DequeueRequestAsync(CancellationToken ct = default)
            => _queueHelper.DequeueRequestAsync(ct);

        public Task<DuplexMessage> DequeueAsync(CancellationToken ct = default)
            => _queueHelper.DequeueAsync(ct);

        public bool TryDequeuePush(out DuplexMessage message)
            => _queueHelper.TryDequeuePush(out message);

        public bool TryDequeueRequest(out DuplexMessage message)
            => _queueHelper.TryDequeueRequest(out message);

        public new void Dispose()
        {
            _queueHelper?.Dispose();
            base.Dispose();
        }
    }

    /// <summary>
    /// キュー機能を内蔵した WebSocketDuplexChannel の例
    /// </summary>
    public class QueuedWebSocketDuplexChannel : WebSocketDuplexChannel, IQueuedDuplexChannel
    {
        private readonly MessageQueueHelper _queueHelper;

        public int PushQueueCount => _queueHelper.PushQueueCount;
        public int RequestQueueCount => _queueHelper.RequestQueueCount;

        public bool UseQueueMode
        {
            get => !_queueHelper.UseEventMode;
            set => _queueHelper.UseEventMode = !value;
        }

        public QueuedWebSocketDuplexChannel(System.Net.WebSockets.WebSocket webSocket, string id = null)
            : base(webSocket, id)
        {
            _queueHelper = new MessageQueueHelper();
            base.OnReceived += (ch, msg) =>
            {
                if (!_queueHelper.UseEventMode)
                {
                    _queueHelper.Enqueue(msg);
                }
            };
        }

        public Task<DuplexMessage> DequeuePushAsync(CancellationToken ct = default)
            => _queueHelper.DequeuePushAsync(ct);

        public Task<DuplexMessage> DequeueRequestAsync(CancellationToken ct = default)
            => _queueHelper.DequeueRequestAsync(ct);

        public Task<DuplexMessage> DequeueAsync(CancellationToken ct = default)
            => _queueHelper.DequeueAsync(ct);

        public bool TryDequeuePush(out DuplexMessage message)
            => _queueHelper.TryDequeuePush(out message);

        public bool TryDequeueRequest(out DuplexMessage message)
            => _queueHelper.TryDequeueRequest(out message);

        public new void Dispose()
        {
            _queueHelper?.Dispose();
            base.Dispose();
        }
    }
}
