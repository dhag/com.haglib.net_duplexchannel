using System;
using System.Threading;
using System.Threading.Tasks;
using HagLib.NET.Duplex;

namespace DuplexQueueExample
{
    /// <summary>
    /// DuplexMessageQueue の使用例
    /// </summary>
    class Program
    {
        static async Task Main(string[] args)
        {
            // 例1: クライアント側でキューベース受信
            await ClientQueueExample();

            // 例2: サーバー側でキューベース受信（Request優先処理）
            await ServerQueueExample();

            // 例3: Push と Request を別々に処理
            await SeparateQueueProcessingExample();
        }

        /// <summary>
        /// 例1: クライアント側でキューベース受信
        /// </summary>
        static async Task ClientQueueExample()
        {
            Console.WriteLine("=== Client Queue Example ===");

            var client = new TcpDuplexClient("localhost", 9000);

            // キューを作成してバインド（ワンライナー）
            var queue = client.CreateBoundQueue();

            try
            {
                await client.ConnectAsync();
                Console.WriteLine("Connected");

                // 別タスクでキューを監視
                var cts = new CancellationTokenSource();
                var receiveTask = Task.Run(async () =>
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        // タイムアウト付きでデキュー
                        var queued = await queue.DequeueAnyAsync(1000, cts.Token);
                        if (queued != null)
                        {
                            Console.WriteLine($"Received from {queued.Channel.Id}: {queued.Message.PayloadString}");

                            // Request の場合は応答
                            if (queued.Message.Type == MessageType.Request)
                            {
                                await queued.ReplyAsync("OK, received your request");
                            }
                        }
                    }
                }, cts.Token);

                // メイン処理
                await client.SendAsync("Hello Server!");
                await Task.Delay(5000);

                cts.Cancel();
                await receiveTask;
            }
            finally
            {
                client.Dispose();
                queue.Dispose();
            }
        }

        /// <summary>
        /// 例2: サーバー側でキューベース受信
        /// </summary>
        static async Task ServerQueueExample()
        {
            Console.WriteLine("=== Server Queue Example ===");

            var server = new TcpDuplexServer();
            var queue = server.CreateBoundQueue();

            try
            {
                await server.StartAsync(9000);
                Console.WriteLine("Server started on port 9000");

                var cts = new CancellationTokenSource();
                
                // ワーカータスク：キューからメッセージを処理
                var workerTask = Task.Run(async () =>
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        // Request を優先的に処理
                        var queued = await queue.DequeueAnyAsync(100, cts.Token);
                        if (queued == null) continue;

                        Console.WriteLine($"[{queued.Channel.Id}] Type={queued.Message.Type}, Content={queued.Message.PayloadString}");

                        if (queued.Message.Type == MessageType.Request)
                        {
                            // リクエストには即座に応答
                            var response = $"Response to: {queued.Message.PayloadString}";
                            await queued.ReplyAsync(response);
                            Console.WriteLine($"  -> Replied: {response}");
                        }
                    }
                }, cts.Token);

                // 10秒間サーバー稼働
                await Task.Delay(10000);

                cts.Cancel();
                await server.StopAsync();
            }
            finally
            {
                server.Dispose();
                queue.Dispose();
            }
        }

        /// <summary>
        /// 例3: Push と Request を別々のワーカーで処理
        /// </summary>
        static async Task SeparateQueueProcessingExample()
        {
            Console.WriteLine("=== Separate Queue Processing Example ===");

            var server = new WebSocketDuplexServer();
            var queue = new DuplexMessageQueue();
            server.BindToQueue(queue);

            try
            {
                await server.StartAsync(8080, "/ws");
                Console.WriteLine("WebSocket server started on ws://localhost:8080/ws");

                var cts = new CancellationTokenSource();

                // Push 処理ワーカー（低優先度、バッチ処理向き）
                var pushWorker = Task.Run(async () =>
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        var queued = await queue.DequeuePushAsync(500, cts.Token);
                        if (queued == null) continue;

                        // Push はログや通知など、非同期でOKな処理
                        Console.WriteLine($"[PUSH] {queued.Channel.Id}: {queued.Message.PayloadString}");
                        
                        // 例: 全クライアントにブロードキャスト（送信者除く）
                        // await server.BroadcastExceptAsync(queued.Channel.Id, queued.Message);
                    }
                }, cts.Token);

                // Request 処理ワーカー（高優先度、即時応答必須）
                var requestWorker = Task.Run(async () =>
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        var queued = await queue.DequeueRequestAsync(100, cts.Token);
                        if (queued == null) continue;

                        Console.WriteLine($"[REQUEST] {queued.Channel.Id}: {queued.Message.PayloadString}");

                        // リクエストは即座に処理して応答
                        try
                        {
                            var result = await ProcessRequestAsync(queued.Message);
                            await queued.ReplyAsync(result);
                            Console.WriteLine($"  -> Replied with result");
                        }
                        catch (Exception ex)
                        {
                            await queued.ReplyAsync($"Error: {ex.Message}");
                            Console.WriteLine($"  -> Replied with error: {ex.Message}");
                        }
                    }
                }, cts.Token);

                // 統計表示
                var statsTask = Task.Run(async () =>
                {
                    while (!cts.Token.IsCancellationRequested)
                    {
                        await Task.Delay(5000, cts.Token);
                        Console.WriteLine($"Queue status - Push: {queue.PushCount}, Request: {queue.RequestCount}");
                    }
                }, cts.Token);

                Console.WriteLine("Press Enter to stop...");
                Console.ReadLine();

                cts.Cancel();
                await server.StopAsync();
            }
            finally
            {
                server.Dispose();
                queue.Dispose();
            }
        }

        static async Task<string> ProcessRequestAsync(DuplexMessage request)
        {
            // 実際の処理をシミュレート
            await Task.Delay(100);
            return $"Processed: {request.PayloadString}";
        }
    }

    /// <summary>
    /// より高度な使い方：マルチスレッドワーカープール
    /// </summary>
    public class QueuedRequestProcessor : IDisposable
    {
        private readonly DuplexMessageQueue _queue;
        private readonly int _workerCount;
        private readonly CancellationTokenSource _cts;
        private readonly Task[] _workers;

        public event Func<QueuedMessage, Task<DuplexMessage>> OnProcessRequest;

        public QueuedRequestProcessor(DuplexMessageQueue queue, int workerCount = 4)
        {
            _queue = queue;
            _workerCount = workerCount;
            _cts = new CancellationTokenSource();
            _workers = new Task[workerCount];
        }

        public void Start()
        {
            for (int i = 0; i < _workerCount; i++)
            {
                var workerId = i;
                _workers[i] = Task.Run(() => WorkerLoop(workerId, _cts.Token));
            }
        }

        private async Task WorkerLoop(int workerId, CancellationToken ct)
        {
            Console.WriteLine($"Worker {workerId} started");

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var queued = await _queue.DequeueRequestAsync(500, ct);
                    if (queued == null) continue;

                    Console.WriteLine($"Worker {workerId} processing request from {queued.Channel.Id}");

                    if (OnProcessRequest != null)
                    {
                        var response = await OnProcessRequest(queued);
                        await queued.Channel.ReplyAsync(queued.Message, response, ct);
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Worker {workerId} error: {ex.Message}");
                }
            }

            Console.WriteLine($"Worker {workerId} stopped");
        }

        public async Task StopAsync()
        {
            _cts.Cancel();
            await Task.WhenAll(_workers);
        }

        public void Dispose()
        {
            _cts.Cancel();
            _cts.Dispose();
        }
    }
}
