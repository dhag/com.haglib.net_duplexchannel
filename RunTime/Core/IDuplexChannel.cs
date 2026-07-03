using System;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// 双方向通信メッセージ
    /// </summary>
    public class DuplexMessage
    {
        /// <summary>メッセージID（リクエスト/レスポンス紐付け用）</summary>
        public int Id { get; set; }

        /// <summary>メッセージ種別</summary>
        public MessageType Type { get; set; }

        /// <summary>メッセージタグ（ルーティング用、任意）</summary>
        public string Tag { get; set; }

        /// <summary>ペイロード（バイナリ）</summary>
        public byte[] Payload { get; set; }

        /// <summary>ペイロードを文字列として取得</summary>
        public string PayloadString
        {
            get => Payload != null ? System.Text.Encoding.UTF8.GetString(Payload) : null;
            set => Payload = value != null ? System.Text.Encoding.UTF8.GetBytes(value) : null;
        }

        public DuplexMessage()
        {
            Tag = "";
        }

        public DuplexMessage(string text) : this()
        {
            Type = MessageType.Push;
            PayloadString = text;
        }

        public DuplexMessage(byte[] data) : this()
        {
            Type = MessageType.Push;
            Payload = data;
        }
    }

    /// <summary>
    /// メッセージ種別
    /// </summary>
    public enum MessageType : byte
    {
        /// <summary>一方的なプッシュ</summary>
        Push = 0,
        /// <summary>応答を期待するリクエスト</summary>
        Request = 1,
        /// <summary>リクエストへの応答</summary>
        Response = 2,
    }

    /// <summary>
    /// 双方向通信チャネルのインターフェース
    /// </summary>
    public interface IDuplexChannel : IDisposable
    {
        /// <summary>接続中かどうか</summary>
        bool IsConnected { get; }

        /// <summary>識別子</summary>
        string Id { get; }

        /// <summary>メッセージ受信イベント（プッシュ/リクエスト）</summary>
        event Action<IDuplexChannel, DuplexMessage> OnReceived;

        /// <summary>切断イベント</summary>
        event Action<IDuplexChannel> OnDisconnected;

        /// <summary>プッシュ送信（応答なし）</summary>
        Task SendAsync(DuplexMessage message, CancellationToken ct = default);

        /// <summary>プッシュ送信（文字列）</summary>
        Task SendAsync(string text, CancellationToken ct = default);

        /// <summary>プッシュ送信（バイナリ）</summary>
        Task SendAsync(byte[] data, CancellationToken ct = default);

        /// <summary>リクエスト送信して応答を待つ</summary>
        Task<DuplexMessage> SendAndReceiveAsync(DuplexMessage message, CancellationToken ct = default);

        /// <summary>リクエスト送信して応答を待つ（文字列）</summary>
        Task<DuplexMessage> SendAndReceiveAsync(string text, CancellationToken ct = default);

        /// <summary>リクエストに応答する</summary>
        Task ReplyAsync(DuplexMessage request, DuplexMessage response, CancellationToken ct = default);

        /// <summary>リクエストに応答する（文字列）</summary>
        Task ReplyAsync(DuplexMessage request, string text, CancellationToken ct = default);

        /// <summary>切断</summary>
        Task CloseAsync();
    }

    /// <summary>
    /// 双方向通信サーバーのインターフェース
    /// </summary>
    public interface IDuplexServer : IDisposable
    {
        /// <summary>リッスン中かどうか</summary>
        bool IsListening { get; }

        /// <summary>接続中のクライアント一覧</summary>
        IDuplexChannel[] Clients { get; }

        /// <summary>クライアント接続イベント</summary>
        event Action<IDuplexChannel> OnClientConnected;

        /// <summary>クライアント切断イベント</summary>
        event Action<IDuplexChannel> OnClientDisconnected;

        /// <summary>リッスン開始</summary>
        Task StartAsync(int port, CancellationToken ct = default);

        /// <summary>全クライアントにブロードキャスト</summary>
        Task BroadcastAsync(DuplexMessage message, CancellationToken ct = default);

        /// <summary>全クライアントにブロードキャスト（文字列）</summary>
        Task BroadcastAsync(string text, CancellationToken ct = default);

        /// <summary>サーバー停止</summary>
        Task StopAsync();
    }
}
