using System;
using System.IO;
using System.Text;


namespace HagLib.NET.Duplex
{
    /// <summary>
    /// パケットフォーマット（バイナリ）
    /// 
    /// ヘッダー (16バイト固定):
    /// [0-3]   Magic "DPX\n" (4バイト)
    /// [4]     Version (1バイト) = 1
    /// [5]     MessageType (1バイト)
    /// [6-9]   MessageId (4バイト, little-endian)
    /// [10-13] PayloadLength (4バイト, little-endian)
    /// [14-15] TagLength (2バイト, little-endian)
    /// 
    /// ボディ:
    /// [16..]  Tag (可変長, UTF-8)
    /// [..]    Payload (可変長)
    /// </summary>
    internal static class DuplexPacket
    {
        public const int HeaderSize = 16;
        public static readonly byte[] Magic = { (byte)'D', (byte)'P', (byte)'X', (byte)'\n' };
        public const byte Version = 1;

        /// <summary>
        /// メッセージをバイト配列にシリアライズ
        /// </summary>
        public static byte[] Serialize(DuplexMessage message)
        {
            var tagBytes = string.IsNullOrEmpty(message.Tag) 
                ? Array.Empty<byte>() 
                : Encoding.UTF8.GetBytes(message.Tag);
            
            var payloadBytes = message.Payload ?? Array.Empty<byte>();

            var totalLength = HeaderSize + tagBytes.Length + payloadBytes.Length;
            var buffer = new byte[totalLength];

            // Magic
            Buffer.BlockCopy(Magic, 0, buffer, 0, 4);
            
            // Version
            buffer[4] = Version;
            
            // MessageType
            buffer[5] = (byte)message.Type;
            
            // MessageId (little-endian)
            buffer[6] = (byte)(message.Id & 0xFF);
            buffer[7] = (byte)((message.Id >> 8) & 0xFF);
            buffer[8] = (byte)((message.Id >> 16) & 0xFF);
            buffer[9] = (byte)((message.Id >> 24) & 0xFF);
            
            // PayloadLength (little-endian)
            buffer[10] = (byte)(payloadBytes.Length & 0xFF);
            buffer[11] = (byte)((payloadBytes.Length >> 8) & 0xFF);
            buffer[12] = (byte)((payloadBytes.Length >> 16) & 0xFF);
            buffer[13] = (byte)((payloadBytes.Length >> 24) & 0xFF);
            
            // TagLength (little-endian)
            buffer[14] = (byte)(tagBytes.Length & 0xFF);
            buffer[15] = (byte)((tagBytes.Length >> 8) & 0xFF);

            // Tag
            if (tagBytes.Length > 0)
            {
                Buffer.BlockCopy(tagBytes, 0, buffer, HeaderSize, tagBytes.Length);
            }

            // Payload
            if (payloadBytes.Length > 0)
            {
                Buffer.BlockCopy(payloadBytes, 0, buffer, HeaderSize + tagBytes.Length, payloadBytes.Length);
            }

            return buffer;
        }

        /// <summary>
        /// ヘッダーを解析（16バイト必要）
        /// </summary>
        public static bool TryParseHeader(byte[] header, out MessageType type, out int messageId, out int payloadLength, out int tagLength)
        {
            type = MessageType.Push;
            messageId = 0;
            payloadLength = 0;
            tagLength = 0;

            if (header == null || header.Length < HeaderSize)
                return false;

            // Magic check
            if (header[0] != Magic[0] || header[1] != Magic[1] || 
                header[2] != Magic[2] || header[3] != Magic[3])
                return false;

            // Version check
            if (header[4] != Version)
                return false;

            type = (MessageType)header[5];
            
            messageId = header[6] | (header[7] << 8) | (header[8] << 16) | (header[9] << 24);
            payloadLength = header[10] | (header[11] << 8) | (header[12] << 16) | (header[13] << 24);
            tagLength = header[14] | (header[15] << 8);

            return true;
        }

        /// <summary>
        /// ボディを解析してメッセージを構築
        /// </summary>
        public static DuplexMessage ParseBody(MessageType type, int messageId, byte[] body, int tagLength)
        {
            var message = new DuplexMessage
            {
                Type = type,
                Id = messageId
            };

            if (tagLength > 0 && body.Length >= tagLength)
            {
                message.Tag = Encoding.UTF8.GetString(body, 0, tagLength);
            }

            var payloadStart = tagLength;
            var payloadLength = body.Length - tagLength;
            if (payloadLength > 0)
            {
                message.Payload = new byte[payloadLength];
                Buffer.BlockCopy(body, payloadStart, message.Payload, 0, payloadLength);
            }

            return message;
        }
    }
}
