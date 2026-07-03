using System;
using System.Collections.Generic;
using UnityEngine;
using HagLib.NET.Duplex;

namespace HagLib.NET.Duplex.Unity
{
    /// <summary>
    /// Unity用サンプル：ParsedMessageQueue の使用例
    /// </summary>
    public class NetworkManager : MonoBehaviour
    {
        [Header("接続設定")]
        [SerializeField] private string _host = "localhost";
        [SerializeField] private int _port = 9000;

        [Header("ワーカー設定")]
        [SerializeField] private int _pushWorkerCount = 1;
        [SerializeField] private int _requestWorkerCount = 2;

        private TcpDuplexClient _client;
        private ParsedMessageQueue<MyData> _queue;

        // バッファ（GC削減のため再利用）
        private readonly List<ParsedItem<MyData>> _pushBuffer = new();
        private readonly List<ParsedItem<MyData>> _requestBuffer = new();

        public bool IsConnected => _client?.IsConnected ?? false;

        public event Action<MyData> OnDataReceived;
        public event Action<ParsedItem<MyData>> OnRequestReceived;
        public event Action OnConnected;
        public event Action OnDisconnected;

        async void Start()
        {
            _client = new TcpDuplexClient(_host, _port);
            _client.OnDisconnected += _ => OnDisconnected?.Invoke();

            // パース済みキューを作成
            _queue = _client.CreateParsedQueue<MyData>(
                parser: ParseMessage,
                pushWorkerCount: _pushWorkerCount,
                requestWorkerCount: _requestWorkerCount
            );

            try
            {
                await _client.ConnectAsync();
                Debug.Log($"Connected to {_host}:{_port}");
                OnConnected?.Invoke();
            }
            catch (Exception ex)
            {
                Debug.LogError($"Connection failed: {ex.Message}");
            }
        }

        void Update()
        {
            if (_queue == null) return;

            // Request を優先処理（応答が必要）
            ProcessRequests();

            // Push を処理
            ProcessPushMessages();
        }

        private void ProcessRequests()
        {
            _requestBuffer.Clear();
            _queue.DequeueAllRequest(_requestBuffer);

            foreach (var item in _requestBuffer)
            {
                try
                {
                    // リクエスト処理イベント発火
                    OnRequestReceived?.Invoke(item);

                    // デフォルト応答（イベントで処理されなかった場合）
                    // item.ReplyAsync("OK");
                }
                catch (Exception ex)
                {
                    Debug.LogError($"Request processing error: {ex.Message}");
                    _ = item.ReplyAsync($"Error: {ex.Message}");
                }
            }
        }

        private void ProcessPushMessages()
        {
            _pushBuffer.Clear();
            _queue.DequeueAllPush(_pushBuffer);

            foreach (var item in _pushBuffer)
            {
                try
                {
                    OnDataReceived?.Invoke(item.Data);
                }
                catch (Exception ex)
                {
                    Debug.LogError($"Push processing error: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// パース関数（ワーカースレッドで実行される）
        /// </summary>
        private MyData ParseMessage(DuplexMessage message)
        {
            // 重いパース処理
            var payload = TypedPayload.FromMessage(message);

            if (payload.Count == 0)
            {
                return new MyData { Text = message.PayloadString };
            }

            var firstItem = payload[0];

            switch (firstItem.Type)
            {
                case ContentType.Json:
                    return JsonUtility.FromJson<MyData>(firstItem.DataString);

                case ContentType.Text:
                    return new MyData { Text = firstItem.DataString };

                case ContentType.Image:
                    return new MyData
                    {
                        ImageData = firstItem.Data,
                        MimeType = firstItem.MimeType
                    };

                case ContentType.Binary:
                    return new MyData { BinaryData = firstItem.Data };

                default:
                    return new MyData { BinaryData = firstItem.Data };
            }
        }

        /// <summary>
        /// データ送信
        /// </summary>
        public async void SendData(MyData data)
        {
            if (!IsConnected) return;

            try
            {
                var json = JsonUtility.ToJson(data);
                await _client.SendJsonAsync(json);
            }
            catch (Exception ex)
            {
                Debug.LogError($"Send error: {ex.Message}");
            }
        }

        /// <summary>
        /// リクエスト送信して応答を待つ
        /// </summary>
        public async void SendRequest(MyData data, Action<MyData> onResponse)
        {
            if (!IsConnected) return;

            try
            {
                var json = JsonUtility.ToJson(data);
                var response = await _client.SendJsonAndReceiveAsync(json);

                var responseData = ParseTypedPayloadToMyData(response);
                onResponse?.Invoke(responseData);
            }
            catch (Exception ex)
            {
                Debug.LogError($"Request error: {ex.Message}");
            }
        }

        private MyData ParseTypedPayloadToMyData(TypedPayload payload)
        {
            if (payload.Count == 0) return new MyData();

            var item = payload[0];
            if (item.Type == ContentType.Json)
            {
                return JsonUtility.FromJson<MyData>(item.DataString);
            }
            return new MyData { Text = item.DataString };
        }

        void OnDestroy()
        {
            _queue?.Dispose();
            _client?.Dispose();
        }

        void OnApplicationQuit()
        {
            _queue?.Dispose();
            _client?.Dispose();
        }
    }

    /// <summary>
    /// サンプルデータクラス
    /// </summary>
    [Serializable]
    public class MyData
    {
        public string Text;
        public int Value;
        public float[] FloatArray;

        [NonSerialized] public byte[] ImageData;
        [NonSerialized] public byte[] BinaryData;
        [NonSerialized] public string MimeType;
    }

    /// <summary>
    /// 使用例：UIコントローラー
    /// </summary>
    public class SampleUIController : MonoBehaviour
    {
        [SerializeField] private NetworkManager _network;
        [SerializeField] private UnityEngine.UI.Text _statusText;
        [SerializeField] private UnityEngine.UI.Text _messageText;

        void Start()
        {
            _network.OnConnected += () =>
            {
                _statusText.text = "Connected";
            };

            _network.OnDisconnected += () =>
            {
                _statusText.text = "Disconnected";
            };

            _network.OnDataReceived += data =>
            {
                _messageText.text = $"Received: {data.Text}";
            };

            _network.OnRequestReceived += async item =>
            {
                // サーバーからのリクエストに応答
                Debug.Log($"Server request: {item.Data.Text}");

                var response = new MyData { Text = "Response from Unity", Value = 42 };
                var json = JsonUtility.ToJson(response);
                await item.ReplyAsync(TypedPayload.FromJson(json));
            };
        }

        public void OnSendButtonClick()
        {
            var data = new MyData { Text = "Hello from Unity", Value = 123 };
            _network.SendData(data);
        }

        public void OnRequestButtonClick()
        {
            var data = new MyData { Text = "Request from Unity" };
            _network.SendRequest(data, response =>
            {
                _messageText.text = $"Response: {response.Text}";
            });
        }
    }
}
