using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// コンテンツ種別
    /// </summary>
    public enum ContentType : byte
    {
        Text = 0,
        Binary = 1,
        Image = 2,
        Json = 3,
        Custom = 255,
    }

    /// <summary>
    /// 型付きペイロード（単一アイテム）
    /// </summary>
    public class TypedPayloadItem
    {
        public ContentType Type { get; set; }
        public string MimeType { get; set; }
        public byte[] Data { get; set; }

        /// <summary>データを文字列として取得（Text/Json用）</summary>
        public string DataString
        {
            get => Data != null ? Encoding.UTF8.GetString(Data) : null;
            set => Data = value != null ? Encoding.UTF8.GetBytes(value) : null;
        }

        public TypedPayloadItem()
        {
            MimeType = "";
            Data = Array.Empty<byte>();
        }

        #region ファクトリメソッド

        public static TypedPayloadItem Text(string text)
        {
            return new TypedPayloadItem
            {
                Type = ContentType.Text,
                MimeType = "text/plain",
                DataString = text
            };
        }

        public static TypedPayloadItem Json(string json)
        {
            return new TypedPayloadItem
            {
                Type = ContentType.Json,
                MimeType = "application/json",
                DataString = json
            };
        }

        public static TypedPayloadItem Image(byte[] imageData, string mimeType = "image/png")
        {
            return new TypedPayloadItem
            {
                Type = ContentType.Image,
                MimeType = mimeType,
                Data = imageData
            };
        }

        public static TypedPayloadItem ImageAuto(byte[] imageData)
        {
            return Image(imageData, DetectImageMimeType(imageData));
        }

        public static TypedPayloadItem Binary(byte[] data, string mimeType = "application/octet-stream")
        {
            return new TypedPayloadItem
            {
                Type = ContentType.Binary,
                MimeType = mimeType,
                Data = data
            };
        }

        public static TypedPayloadItem Custom(byte[] data, string mimeType)
        {
            return new TypedPayloadItem
            {
                Type = ContentType.Custom,
                MimeType = mimeType,
                Data = data
            };
        }

        #endregion

        #region シリアライズ（単一アイテム）

        /// <summary>
        /// 単一アイテムをシリアライズ
        /// 形式: [ContentType:1][MimeLen:2][Mime...][Data...]
        /// </summary>
        public byte[] Serialize()
        {
            var mimeBytes = string.IsNullOrEmpty(MimeType)
                ? Array.Empty<byte>()
                : Encoding.UTF8.GetBytes(MimeType);

            var dataBytes = Data ?? Array.Empty<byte>();

            var totalLength = 3 + mimeBytes.Length + dataBytes.Length;
            var buffer = new byte[totalLength];

            buffer[0] = (byte)Type;
            buffer[1] = (byte)(mimeBytes.Length & 0xFF);
            buffer[2] = (byte)((mimeBytes.Length >> 8) & 0xFF);

            if (mimeBytes.Length > 0)
                Buffer.BlockCopy(mimeBytes, 0, buffer, 3, mimeBytes.Length);

            if (dataBytes.Length > 0)
                Buffer.BlockCopy(dataBytes, 0, buffer, 3 + mimeBytes.Length, dataBytes.Length);

            return buffer;
        }

        #endregion

        #region ユーティリティ

        public static string DetectImageMimeType(byte[] data)
        {
            if (data == null || data.Length < 4)
                return "application/octet-stream";

            if (data[0] == 0x89 && data[1] == 0x50 && data[2] == 0x4E && data[3] == 0x47)
                return "image/png";
            if (data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF)
                return "image/jpeg";
            if (data[0] == 0x47 && data[1] == 0x49 && data[2] == 0x46 && data[3] == 0x38)
                return "image/gif";
            if (data[0] == 0x42 && data[1] == 0x4D)
                return "image/bmp";
            if (data.Length >= 12 && data[0] == 0x52 && data[1] == 0x49 && data[2] == 0x46 && data[3] == 0x46
                && data[8] == 0x57 && data[9] == 0x45 && data[10] == 0x42 && data[11] == 0x50)
                return "image/webp";

            return "application/octet-stream";
        }

        public override string ToString()
        {
            return $"Item {{ Type={Type}, Mime=\"{MimeType}\", Size={Data?.Length ?? 0} }}";
        }

        #endregion
    }

    /// <summary>
    /// 型付きペイロードリスト（複数アイテム混在）
    /// 
    /// シリアライズ形式:
    /// [0-3]    アイテム数 (4バイト, little-endian)
    /// 各アイテム:
    ///   [0-3]    アイテムサイズ (4バイト, little-endian)
    ///   [4]      ContentType (1バイト)
    ///   [5-6]    MimeType長 (2バイト, little-endian)
    ///   [7..]    MimeType (UTF-8)
    ///   [..]     Data
    /// </summary>
    public class TypedPayload : IEnumerable<TypedPayloadItem>
    {
        private readonly List<TypedPayloadItem> _items = new List<TypedPayloadItem>();

        /// <summary>アイテム数</summary>
        public int Count => _items.Count;

        /// <summary>インデクサ</summary>
        public TypedPayloadItem this[int index] => _items[index];

        #region コンストラクタ / ファクトリ

        public TypedPayload() { }

        /// <summary>単一アイテムから作成</summary>
        public TypedPayload(TypedPayloadItem item)
        {
            _items.Add(item);
        }

        /// <summary>複数アイテムから作成</summary>
        public TypedPayload(IEnumerable<TypedPayloadItem> items)
        {
            _items.AddRange(items);
        }

        /// <summary>テキスト1件から作成</summary>
        public static TypedPayload FromText(string text)
        {
            return new TypedPayload(TypedPayloadItem.Text(text));
        }

        /// <summary>JSON1件から作成</summary>
        public static TypedPayload FromJson(string json)
        {
            return new TypedPayload(TypedPayloadItem.Json(json));
        }

        /// <summary>画像1件から作成</summary>
        public static TypedPayload FromImage(byte[] imageData, string mimeType = "image/png")
        {
            return new TypedPayload(TypedPayloadItem.Image(imageData, mimeType));
        }

        /// <summary>バイナリ1件から作成</summary>
        public static TypedPayload FromBinary(byte[] data, string mimeType = "application/octet-stream")
        {
            return new TypedPayload(TypedPayloadItem.Binary(data, mimeType));
        }

        /// <summary>カスタム1件から作成</summary>
        public static TypedPayload FromCustom(byte[] data, string mimeType)
        {
            return new TypedPayload(TypedPayloadItem.Custom(data, mimeType));
        }

        #endregion

        #region アイテム操作

        public TypedPayload Add(TypedPayloadItem item)
        {
            _items.Add(item);
            return this; // チェーン可能
        }

        public TypedPayload AddText(string text)
        {
            return Add(TypedPayloadItem.Text(text));
        }

        public TypedPayload AddJson(string json)
        {
            return Add(TypedPayloadItem.Json(json));
        }

        public TypedPayload AddImage(byte[] imageData, string mimeType = "image/png")
        {
            return Add(TypedPayloadItem.Image(imageData, mimeType));
        }

        public TypedPayload AddImageAuto(byte[] imageData)
        {
            return Add(TypedPayloadItem.ImageAuto(imageData));
        }

        public TypedPayload AddBinary(byte[] data, string mimeType = "application/octet-stream")
        {
            return Add(TypedPayloadItem.Binary(data, mimeType));
        }

        public TypedPayload AddCustom(byte[] data, string mimeType)
        {
            return Add(TypedPayloadItem.Custom(data, mimeType));
        }

        public void Clear()
        {
            _items.Clear();
        }

        #endregion

        #region 検索・取得

        /// <summary>指定した型の最初のアイテムを取得</summary>
        public TypedPayloadItem GetFirst(ContentType type)
        {
            foreach (var item in _items)
            {
                if (item.Type == type)
                    return item;
            }
            return null;
        }

        /// <summary>指定したMIMEタイプの最初のアイテムを取得</summary>
        public TypedPayloadItem GetFirstByMime(string mimeType)
        {
            foreach (var item in _items)
            {
                if (item.MimeType == mimeType)
                    return item;
            }
            return null;
        }

        /// <summary>指定した型のアイテム一覧を取得</summary>
        public IEnumerable<TypedPayloadItem> GetAll(ContentType type)
        {
            foreach (var item in _items)
            {
                if (item.Type == type)
                    yield return item;
            }
        }

        /// <summary>最初のテキストを取得</summary>
        public string GetText() => GetFirst(ContentType.Text)?.DataString;

        /// <summary>最初のJSONを取得</summary>
        public string GetJson() => GetFirst(ContentType.Json)?.DataString;

        /// <summary>最初の画像データを取得</summary>
        public byte[] GetImage() => GetFirst(ContentType.Image)?.Data;

        /// <summary>最初の画像のMIMEタイプを取得</summary>
        public string GetImageMimeType() => GetFirst(ContentType.Image)?.MimeType;

        /// <summary>最初のバイナリデータを取得</summary>
        public byte[] GetBinary() => GetFirst(ContentType.Binary)?.Data;

        /// <summary>全てのテキストを取得</summary>
        public IEnumerable<string> GetAllTexts()
        {
            foreach (var item in GetAll(ContentType.Text))
                yield return item.DataString;
        }

        /// <summary>全てのJSONを取得</summary>
        public IEnumerable<string> GetAllJson()
        {
            foreach (var item in GetAll(ContentType.Json))
                yield return item.DataString;
        }

        /// <summary>全ての画像を取得</summary>
        public IEnumerable<(byte[] Data, string MimeType)> GetAllImages()
        {
            foreach (var item in GetAll(ContentType.Image))
                yield return (item.Data, item.MimeType);
        }

        /// <summary>全てのバイナリを取得</summary>
        public IEnumerable<(byte[] Data, string MimeType)> GetAllBinaries()
        {
            foreach (var item in GetAll(ContentType.Binary))
                yield return (item.Data, item.MimeType);
        }

        #endregion

        #region シリアライズ

        /// <summary>バイト配列にシリアライズ</summary>
        public byte[] Serialize()
        {
            // 各アイテムをシリアライズ
            var serializedItems = new List<byte[]>();
            var totalSize = 4; // アイテム数(4バイト)

            foreach (var item in _items)
            {
                var itemBytes = item.Serialize();
                serializedItems.Add(itemBytes);
                totalSize += 4 + itemBytes.Length; // サイズ(4) + データ
            }

            var buffer = new byte[totalSize];
            var offset = 0;

            // アイテム数
            buffer[offset++] = (byte)(_items.Count & 0xFF);
            buffer[offset++] = (byte)((_items.Count >> 8) & 0xFF);
            buffer[offset++] = (byte)((_items.Count >> 16) & 0xFF);
            buffer[offset++] = (byte)((_items.Count >> 24) & 0xFF);

            // 各アイテム
            foreach (var itemBytes in serializedItems)
            {
                // アイテムサイズ
                var len = itemBytes.Length;
                buffer[offset++] = (byte)(len & 0xFF);
                buffer[offset++] = (byte)((len >> 8) & 0xFF);
                buffer[offset++] = (byte)((len >> 16) & 0xFF);
                buffer[offset++] = (byte)((len >> 24) & 0xFF);

                // アイテムデータ
                Buffer.BlockCopy(itemBytes, 0, buffer, offset, len);
                offset += len;
            }

            return buffer;
        }

        /// <summary>バイト配列からデシリアライズ</summary>
        public static TypedPayload Deserialize(byte[] data)
        {
            if (data == null || data.Length < 4)
                throw new ArgumentException("Invalid payload data: too short");

            var payload = new TypedPayload();
            var offset = 0;

            // アイテム数
            var itemCount = data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24);
            offset += 4;

            for (int i = 0; i < itemCount; i++)
            {
                if (offset + 4 > data.Length)
                    throw new ArgumentException("Invalid payload data: item size truncated");

                // アイテムサイズ
                var itemSize = data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24);
                offset += 4;

                if (offset + itemSize > data.Length)
                    throw new ArgumentException("Invalid payload data: item data truncated");

                // アイテムデータを抽出
                var itemData = new byte[itemSize];
                Buffer.BlockCopy(data, offset, itemData, 0, itemSize);

                var item = DeserializeItem(itemData);
                payload._items.Add(item);

                offset += itemSize;
            }

            return payload;
        }

        private static TypedPayloadItem DeserializeItem(byte[] data)
        {
            if (data == null || data.Length < 3)
                throw new ArgumentException("Invalid item data: too short");

            var item = new TypedPayloadItem();

            item.Type = (ContentType)data[0];
            var mimeLength = data[1] | (data[2] << 8);

            if (data.Length < 3 + mimeLength)
                throw new ArgumentException("Invalid item data: mime type truncated");

            if (mimeLength > 0)
                item.MimeType = Encoding.UTF8.GetString(data, 3, mimeLength);
            else
                item.MimeType = "";

            var dataStart = 3 + mimeLength;
            var dataLength = data.Length - dataStart;
            if (dataLength > 0)
            {
                item.Data = new byte[dataLength];
                Buffer.BlockCopy(data, dataStart, item.Data, 0, dataLength);
            }
            else
            {
                item.Data = Array.Empty<byte>();
            }

            return item;
        }

        #endregion

        #region DuplexMessage 変換

        public DuplexMessage ToMessage()
        {
            return new DuplexMessage(Serialize());
        }

        public static TypedPayload FromMessage(DuplexMessage message)
        {
            if (message?.Payload == null || message.Payload.Length == 0)
                return new TypedPayload();

            return Deserialize(message.Payload);
        }

        #endregion

        #region IEnumerable

        public IEnumerator<TypedPayloadItem> GetEnumerator() => _items.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        #endregion

        public override string ToString()
        {
            return $"TypedPayload {{ Count={_items.Count} }}";
        }
    }

    /// <summary>
    /// IDuplexChannel の TypedPayload 用拡張メソッド
    /// </summary>
    public static class DuplexChannelExtensions
    {
        #region 送信（プッシュ）

        public static Task SendAsync(this IDuplexChannel channel, TypedPayload payload, CancellationToken ct = default)
        {
            return channel.SendAsync(payload.Serialize(), ct);
        }

        public static Task SendTextAsync(this IDuplexChannel channel, string text, CancellationToken ct = default)
        {
            return channel.SendAsync(TypedPayload.FromText(text).Serialize(), ct);
        }

        public static Task SendJsonAsync(this IDuplexChannel channel, string json, CancellationToken ct = default)
        {
            return channel.SendAsync(TypedPayload.FromJson(json).Serialize(), ct);
        }

        public static Task SendImageAsync(this IDuplexChannel channel, byte[] imageData, string mimeType = "image/png", CancellationToken ct = default)
        {
            return channel.SendAsync(TypedPayload.FromImage(imageData, mimeType).Serialize(), ct);
        }

        public static Task SendBinaryAsync(this IDuplexChannel channel, byte[] data, string mimeType = "application/octet-stream", CancellationToken ct = default)
        {
            return channel.SendAsync(TypedPayload.FromBinary(data, mimeType).Serialize(), ct);
        }

        #endregion

        #region リクエスト/レスポンス

        public static async Task<TypedPayload> SendAndReceiveAsync(this IDuplexChannel channel, TypedPayload payload, CancellationToken ct = default)
        {
            var response = await channel.SendAndReceiveAsync(payload.ToMessage(), ct).ConfigureAwait(false);
            return TypedPayload.FromMessage(response);
        }

        public static async Task<TypedPayload> SendTextAndReceiveAsync(this IDuplexChannel channel, string text, CancellationToken ct = default)
        {
            var response = await channel.SendAndReceiveAsync(TypedPayload.FromText(text).ToMessage(), ct).ConfigureAwait(false);
            return TypedPayload.FromMessage(response);
        }

        public static async Task<TypedPayload> SendJsonAndReceiveAsync(this IDuplexChannel channel, string json, CancellationToken ct = default)
        {
            var response = await channel.SendAndReceiveAsync(TypedPayload.FromJson(json).ToMessage(), ct).ConfigureAwait(false);
            return TypedPayload.FromMessage(response);
        }

        #endregion

        #region 応答

        public static Task ReplyAsync(this IDuplexChannel channel, DuplexMessage request, TypedPayload payload, CancellationToken ct = default)
        {
            return channel.ReplyAsync(request, payload.ToMessage(), ct);
        }

        public static Task ReplyTextAsync(this IDuplexChannel channel, DuplexMessage request, string text, CancellationToken ct = default)
        {
            return channel.ReplyAsync(request, TypedPayload.FromText(text).ToMessage(), ct);
        }

        public static Task ReplyJsonAsync(this IDuplexChannel channel, DuplexMessage request, string json, CancellationToken ct = default)
        {
            return channel.ReplyAsync(request, TypedPayload.FromJson(json).ToMessage(), ct);
        }

        public static Task ReplyImageAsync(this IDuplexChannel channel, DuplexMessage request, byte[] imageData, string mimeType = "image/png", CancellationToken ct = default)
        {
            return channel.ReplyAsync(request, TypedPayload.FromImage(imageData, mimeType).ToMessage(), ct);
        }

        #endregion

        #region 変換

        public static TypedPayload ToTypedPayload(this DuplexMessage message)
        {
            return TypedPayload.FromMessage(message);
        }

        #endregion
    }

    /// <summary>
    /// IDuplexServer の TypedPayload 用拡張メソッド
    /// </summary>
    public static class DuplexServerExtensions
    {
        public static Task BroadcastAsync(this IDuplexServer server, TypedPayload payload, CancellationToken ct = default)
        {
            return server.BroadcastAsync(payload.ToMessage(), ct);
        }

        public static Task BroadcastTextAsync(this IDuplexServer server, string text, CancellationToken ct = default)
        {
            return server.BroadcastAsync(TypedPayload.FromText(text).ToMessage(), ct);
        }

        public static Task BroadcastJsonAsync(this IDuplexServer server, string json, CancellationToken ct = default)
        {
            return server.BroadcastAsync(TypedPayload.FromJson(json).ToMessage(), ct);
        }

        public static Task BroadcastImageAsync(this IDuplexServer server, byte[] imageData, string mimeType = "image/png", CancellationToken ct = default)
        {
            return server.BroadcastAsync(TypedPayload.FromImage(imageData, mimeType).ToMessage(), ct);
        }
    }
}