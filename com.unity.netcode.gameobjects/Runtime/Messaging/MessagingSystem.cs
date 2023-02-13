using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using UnityEngine;
#if WITH_PING_TRACE
using Keepsake.Logging;
using Keepsake.Logging.Data;
#endif

namespace Unity.Netcode
{

internal class InvalidMessageStructureException : SystemException
{
    public InvalidMessageStructureException()
    {
    }

    public InvalidMessageStructureException(string issue) : base(issue)
    {
    }
}

// KEEPSAKE FIX - made public so external message classes can have their ILPP generated code access
public class MessagingSystem : IDisposable
{
    // KEEPSAKE FIX made public
    public delegate void MessageHandler(FastBufferReader reader, ref NetworkContext context, MessagingSystem system);

    private int? m_CurrentLatencyTrace;
    private bool m_Disposed;

    private readonly List<INetworkHooks> m_Hooks = new();

    private NativeList<ReceiveQueueItem> m_IncomingMessageQueue = new(16, Allocator.Persistent);

    private readonly IMessageSender m_MessageSender;

    private readonly Dictionary<Type, uint>                       m_MessageTypes = new();
    private readonly object                                       m_Owner;
    private readonly Dictionary<ulong, NativeList<SendQueueItem>> m_SendQueues = new();

    internal Type[] MessageTypes { get; } = new Type[255];

    internal MessageHandler[] MessageHandlers { get; } = new MessageHandler[255];

    internal uint MessageHandlerCount { get; private set; }

    public MessagingSystem(IMessageSender messageSender, object owner, IMessageProvider provider = null)
    {
        try
        {
            m_MessageSender = messageSender;
            m_Owner = owner;

            if (provider == null)
            {
                provider = new ILPPMessageProvider();
            }
            var allowedTypes = provider.GetMessages();

            allowedTypes.Sort((a, b) => string.CompareOrdinal(a.MessageType.FullName, b.MessageType.FullName));
            foreach (var type in allowedTypes)
            {
                RegisterMessageType(type);
            }
        }
        catch (Exception)
        {
            Dispose();
            throw;
        }
    }

    public unsafe void Dispose()
    {
        if (m_Disposed)
        {
            return;
        }

        // Can't just iterate SendQueues or SendQueues.Keys because ClientDisconnected removes
        // from the queue.
        foreach (var kvp in m_SendQueues)
        {
            CleanupDisconnectedClient(kvp.Key);
        }

        for (var queueIndex = 0; queueIndex < m_IncomingMessageQueue.Length; ++queueIndex)
        {
            // Avoid copies...
            ref var item = ref m_IncomingMessageQueue.GetUnsafeList()->ElementAt(queueIndex);
            item.Reader.Dispose();
        }

        m_IncomingMessageQueue.Dispose();
        m_Disposed = true;
    }

    internal uint GetMessageType(Type t)
    {
        return m_MessageTypes[t];
    }

    public const int NON_FRAGMENTED_MESSAGE_MAX_SIZE = 1170; // KEEPSAKE FIX - EOS 1170 MTU. This might need to be smaller to account for Transport headers?

    // KEEPSAKE FIX - Just because system will accept messages of this size doesn't mean its a good idea.
    //                If changed also update UnityTransport and EpicNetcodeTransport's InitialMaxPayloadSize
    public const int FRAGMENTED_MESSAGE_MAX_SIZE = 50 * 1024 * 1024; // ^^^^^ !!! ^^^^^

    ~MessagingSystem()
    {
        Dispose();
    }

    public void Hook(INetworkHooks hooks)
    {
        m_Hooks.Add(hooks);
    }

    public void Unhook(INetworkHooks hooks)
    {
        m_Hooks.Remove(hooks);
    }

    private void RegisterMessageType(MessageWithHandler messageWithHandler)
    {
        MessageHandlers[MessageHandlerCount] = messageWithHandler.Handler;
        MessageTypes[MessageHandlerCount] = messageWithHandler.MessageType;
        m_MessageTypes[messageWithHandler.MessageType] = MessageHandlerCount++;
    }

    internal void HandleIncomingData(ulong clientId, ArraySegment<byte> data, float receiveTime)
    {
        unsafe
        {
            fixed (byte* nativeData = data.Array)
            {
                var batchReader = new FastBufferReader(nativeData + data.Offset, Allocator.None, data.Count);
                if (!batchReader.TryBeginRead(sizeof(BatchHeader)))
                {
                    NetworkLog.LogWarning("Received a packet too small to contain a BatchHeader. Ignoring it.");
                    return;
                }

                batchReader.ReadValue(out BatchHeader batchHeader);

                for (var hookIdx = 0; hookIdx < m_Hooks.Count; ++hookIdx)
                {
                    m_Hooks[hookIdx].OnBeforeReceiveBatch(clientId, batchHeader.BatchSize, batchReader.Length);
                }

                // PING TRACE
                #if WITH_PING_TRACE
                NetworkTracer.Instance.Check(nativeData, data.Offset, data.Count, "Netcode MessagingSystem handling incoming batch data, adding to internal queue", OnNetworkTracerFoundIncomingQueued);
                #endif
                // END PING TRACE

                for (var messageIdx = 0; messageIdx < batchHeader.BatchSize; ++messageIdx)
                {
                    var messageHeader = new MessageHeader();
                    var position = batchReader.Position;
                    try
                    {
                        ByteUnpacker.ReadValueBitPacked(batchReader, out messageHeader.MessageType);
                        ByteUnpacker.ReadValueBitPacked(batchReader, out messageHeader.MessageSize);
                    }
                    catch (OverflowException)
                    {
                        NetworkLog.LogWarning("Received a batch that didn't have enough data for all of its batches, ending early!");
                        throw;
                    }

                    var receivedHeaderSize = batchReader.Position - position;

                    if (!batchReader.TryBeginRead((int)messageHeader.MessageSize))
                    {
                        NetworkLog.LogWarning("Received a message that claimed a size larger than the packet, ending early!");
                        return;
                    }

                    #if WITH_PING_TRACE
                    if (m_CurrentLatencyTrace.HasValue)
                    {
                        LogGlobal.InfoStructured(m_CurrentLatencyTrace.Value, "Netcode MessagingSystem processing received message batch, chopping up to and queueing individual message", NetworkPacketLogEntry.Create(-1, batchReader.GetUnsafePtrAtCurrentPosition(), (int)messageHeader.MessageSize, false, (int)messageHeader.MessageType));
                    }
                    #endif

                    m_IncomingMessageQueue.Add(
                        new ReceiveQueueItem
                        {
                            Header = messageHeader,
                            SenderId = clientId,
                            Timestamp = receiveTime,
                            // Copy the data for this message into a new FastBufferReader that owns that memory.
                            // We can't guarantee the memory in the ArraySegment stays valid because we don't own it,
                            // so we must move it to memory we do own.
                            Reader = new FastBufferReader(batchReader.GetUnsafePtrAtCurrentPosition(), Allocator.TempJob, (int)messageHeader.MessageSize),
                            MessageHeaderSerializedSize = receivedHeaderSize,
                        });
                    batchReader.Seek(batchReader.Position + (int)messageHeader.MessageSize);
                }
                for (var hookIdx = 0; hookIdx < m_Hooks.Count; ++hookIdx)
                {
                    m_Hooks[hookIdx].OnAfterReceiveBatch(clientId, batchHeader.BatchSize, batchReader.Length);
                }
            }
        }
    }

    private bool CanReceive(ulong clientId, Type messageType)
    {
        for (var hookIdx = 0; hookIdx < m_Hooks.Count; ++hookIdx)
        {
            if (!m_Hooks[hookIdx].OnVerifyCanReceive(clientId, messageType))
            {
                return false;
            }
        }

        return true;
    }

    public void HandleMessage(in MessageHeader header, FastBufferReader reader, ulong senderId, float timestamp, int serializedHeaderSize)
    {
        var context = new NetworkContext
        {
            SystemOwner = m_Owner,
            SenderId = senderId,
            Timestamp = timestamp,
            Header = header,
            SerializedHeaderSize = serializedHeaderSize,
            MessageSize = header.MessageSize,
        };
        if (header.MessageType >= MessageHandlerCount)
        {
            Debug.LogWarning($"Received a message with invalid message type value {header.MessageType}");
            reader.Dispose();
            return;
        }

        var type = MessageTypes[header.MessageType];
        if (!CanReceive(senderId, type))
        {
            reader.Dispose();
            return;
        }

        for (var hookIdx = 0; hookIdx < m_Hooks.Count; ++hookIdx)
        {
            m_Hooks[hookIdx].OnBeforeReceiveMessage(senderId, type, reader.Length + FastBufferWriter.GetWriteSize<MessageHeader>());
        }

        var handler = MessageHandlers[header.MessageType];

        // PING TRACE
        #if WITH_PING_TRACE
        unsafe
        {
            NetworkTracer.Instance.Check(reader.GetUnsafePtr(), 0, reader.Length, $"Netcode MessagingSystem processing received message, sending to handler {handler.GetType().Name}", OnNetworkTracerFoundIncomingSendToHandler);
            if (m_CurrentLatencyTrace.HasValue)
            {
                LogGlobal.InfoStructured(m_CurrentLatencyTrace.Value, $"Netcode MessagingSystem handling message, giving to {handler.GetType().Name}", NetworkPacketLogEntry.Create(-1, reader.GetUnsafePtr(), reader.Length, false, (int)header.MessageType));
            }
        }
        #endif
        // END PING TRACE

        using (reader)
        {
            // No user-land message handler exceptions should escape the receive loop.
            // If an exception is throw, the message is ignored.
            // Example use case: A bad message is received that can't be deserialized and throws
            // an OverflowException because it specifies a length greater than the number of bytes in it
            // for some dynamic-length value.
            try
            {
                handler.Invoke(reader, ref context, this);
            }
            catch (Exception e)
            {
                Debug.LogException(e);
            }
        }
        for (var hookIdx = 0; hookIdx < m_Hooks.Count; ++hookIdx)
        {
            m_Hooks[hookIdx].OnAfterReceiveMessage(senderId, type, reader.Length + FastBufferWriter.GetWriteSize<MessageHeader>());
        }
    }

    internal unsafe void ProcessIncomingMessageQueue()
    {
        for (var index = 0; index < m_IncomingMessageQueue.Length; ++index)
        {
            // Avoid copies...
            ref var item = ref m_IncomingMessageQueue.GetUnsafeList()->ElementAt(index);
            HandleMessage(item.Header, item.Reader, item.SenderId, item.Timestamp, item.MessageHeaderSerializedSize);
            if (m_Disposed)
            {
                return;
            }
        }

        m_IncomingMessageQueue.Clear();
    }

    internal void ClientConnected(ulong clientId)
    {
        if (m_SendQueues.ContainsKey(clientId))
        {
            return;
        }
        m_SendQueues[clientId] = new NativeList<SendQueueItem>(16, Allocator.Persistent);
    }

    internal void ClientDisconnected(ulong clientId)
    {
        if (!m_SendQueues.ContainsKey(clientId))
        {
            return;
        }
        CleanupDisconnectedClient(clientId);
        m_SendQueues.Remove(clientId);
    }

    private unsafe void CleanupDisconnectedClient(ulong clientId)
    {
        var queue = m_SendQueues[clientId];
        for (var i = 0; i < queue.Length; ++i)
        {
            queue.GetUnsafeList()->ElementAt(i).Writer.Dispose();
        }

        queue.Dispose();
    }

    public static void ReceiveMessage<T>(FastBufferReader reader, ref NetworkContext context, MessagingSystem system) where T : INetworkMessage, new()
    {
        var message = new T();
        if (message.Deserialize(reader, ref context))
        {
            for (var hookIdx = 0; hookIdx < system.m_Hooks.Count; ++hookIdx)
            {
                system.m_Hooks[hookIdx].OnBeforeHandleMessage(ref message, ref context);
            }

            message.Handle(ref context);

            for (var hookIdx = 0; hookIdx < system.m_Hooks.Count; ++hookIdx)
            {
                system.m_Hooks[hookIdx].OnAfterHandleMessage(ref message, ref context);
            }
        }
    }

    private bool CanSend(ulong clientId, Type messageType, NetworkDelivery delivery)
    {
        for (var hookIdx = 0; hookIdx < m_Hooks.Count; ++hookIdx)
        {
            if (!m_Hooks[hookIdx].OnVerifyCanSend(clientId, messageType, delivery))
            {
                return false;
            }
        }

        return true;
    }

    internal unsafe int SendMessage<TMessageType, TClientIdListType>(ref TMessageType message, NetworkDelivery delivery, in TClientIdListType clientIds) where TMessageType : INetworkMessage where TClientIdListType : IReadOnlyList<ulong>
    {
        if (clientIds.Count == 0)
        {
            return 0;
        }

        var maxSize = delivery == NetworkDelivery.ReliableFragmentedSequenced ? FRAGMENTED_MESSAGE_MAX_SIZE : NON_FRAGMENTED_MESSAGE_MAX_SIZE;

        using var tmpSerializer = new FastBufferWriter(NON_FRAGMENTED_MESSAGE_MAX_SIZE - FastBufferWriter.GetWriteSize<MessageHeader>(), Allocator.Temp, maxSize - FastBufferWriter.GetWriteSize<MessageHeader>());

        message.Serialize(tmpSerializer);

        using var headerSerializer = new FastBufferWriter(FastBufferWriter.GetWriteSize<MessageHeader>(), Allocator.Temp);

        var header = new MessageHeader
        {
            MessageSize = (uint)tmpSerializer.Length, // KEEPSAKE FIX - cast to uint instead of ushort to not truncate large payloads (>65k)
            MessageType = m_MessageTypes[typeof(TMessageType)],
        };
        BytePacker.WriteValueBitPacked(headerSerializer, header.MessageType);
        BytePacker.WriteValueBitPacked(headerSerializer, header.MessageSize);

        for (var i = 0; i < clientIds.Count; ++i)
        {
            var clientId = clientIds[i];

            if (!CanSend(clientId, typeof(TMessageType), delivery))
            {
                continue;
            }

            for (var hookIdx = 0; hookIdx < m_Hooks.Count; ++hookIdx)
            {
                m_Hooks[hookIdx].OnBeforeSendMessage(clientId, ref message, delivery);
            }

            var sendQueueItem = m_SendQueues[clientId];
            if (sendQueueItem.Length == 0)
            {
                sendQueueItem.Add(new SendQueueItem(delivery, NON_FRAGMENTED_MESSAGE_MAX_SIZE, Allocator.TempJob, maxSize));
                sendQueueItem.GetUnsafeList()->ElementAt(0).Writer.Seek(sizeof(BatchHeader));
            }
            else
            {
                ref var lastQueueItem = ref sendQueueItem.GetUnsafeList()->ElementAt(sendQueueItem.Length - 1);
                if (lastQueueItem.NetworkDelivery != delivery || lastQueueItem.Writer.MaxCapacity - lastQueueItem.Writer.Position < tmpSerializer.Length + headerSerializer.Length)
                {
                    sendQueueItem.Add(new SendQueueItem(delivery, NON_FRAGMENTED_MESSAGE_MAX_SIZE, Allocator.TempJob, maxSize));
                    sendQueueItem.GetUnsafeList()->ElementAt(sendQueueItem.Length - 1).Writer.Seek(sizeof(BatchHeader));
                }
            }

            ref var writeQueueItem = ref sendQueueItem.GetUnsafeList()->ElementAt(sendQueueItem.Length - 1);
            writeQueueItem.Writer.TryBeginWrite(tmpSerializer.Length + headerSerializer.Length);

            writeQueueItem.Writer.WriteBytes(headerSerializer.GetUnsafePtr(), headerSerializer.Length);
            writeQueueItem.Writer.WriteBytes(tmpSerializer.GetUnsafePtr(), tmpSerializer.Length);
            //if (tmpSerializer.Length > 100)
            //{
            //    Debug.Log($"MessageSystem writing {tmpSerializer.Length} byte(s) to send queue -- {BytesToHexString(tmpSerializer.ToArray())}");
            //}

            // PING TRACE
            #if WITH_PING_TRACE
            NetworkTracer.Instance.Check(tmpSerializer.GetUnsafePtr(), 0, tmpSerializer.Length, $"Netcode MessagingSystem queueing to send queue for client {clientId}", OnNetworkTracerFoundOutgoingQueued);
            if (m_CurrentLatencyTrace.HasValue)
            {
                LogGlobal.InfoStructured(m_CurrentLatencyTrace.Value, $"Netcode MessagingSystem SendMessage {message.GetType().Name}", NetworkPacketLogEntry.Create(-1, tmpSerializer.GetUnsafePtr(), tmpSerializer.Length, true, (int)header.MessageType));
            }
            #endif
            // END PING TRACE

            writeQueueItem.BatchHeader.BatchSize++;
            for (var hookIdx = 0; hookIdx < m_Hooks.Count; ++hookIdx)
            {
                m_Hooks[hookIdx].OnAfterSendMessage(clientId, ref message, delivery, tmpSerializer.Length + headerSerializer.Length);
            }
        }

        return tmpSerializer.Length + headerSerializer.Length;
    }

    internal unsafe int SendMessage<T>(ref T message, NetworkDelivery delivery, ulong* clientIds, int numClientIds) where T : INetworkMessage
    {
        return SendMessage(ref message, delivery, new PointerListWrapper<ulong>(clientIds, numClientIds));
    }

    internal unsafe int SendMessage<T>(ref T message, NetworkDelivery delivery, ulong clientId) where T : INetworkMessage
    {
        var clientIds = stackalloc ulong[] { clientId };
        return SendMessage(ref message, delivery, new PointerListWrapper<ulong>(clientIds, 1));
    }

    internal unsafe int SendMessage<T>(ref T message, NetworkDelivery delivery, in NativeArray<ulong> clientIds) where T : INetworkMessage
    {
        return SendMessage(ref message, delivery, new PointerListWrapper<ulong>((ulong*)clientIds.GetUnsafePtr(), clientIds.Length));
    }

    internal unsafe void ProcessSendQueues()
    {
        foreach (var kvp in m_SendQueues)
        {
            var clientId = kvp.Key;
            var sendQueueItem = kvp.Value;
            for (var i = 0; i < sendQueueItem.Length; ++i)
            {
                ref var queueItem = ref sendQueueItem.GetUnsafeList()->ElementAt(i);
                if (queueItem.BatchHeader.BatchSize == 0)
                {
                    queueItem.Writer.Dispose();
                    continue;
                }

                //if (queueItem.Writer.Length > 100)
                //{
                //    Debug.Log($"MessageSystem moving {queueItem.Writer.Length} byte(s) from send queue to message sender");
                //}

                // PING TRACE
                #if WITH_PING_TRACE
                NetworkTracer.Instance.Check(queueItem.Writer.GetUnsafePtr(), 0, queueItem.Writer.Length, "Netcode MessagingSystem dequeuing from client send queue and passing to m_MessageSender", OnNetworkTracerFoundOutgoingDispatched);
                if (m_CurrentLatencyTrace.HasValue)
                {
                    LogGlobal.InfoStructured(m_CurrentLatencyTrace.Value, "Netcode MessagingSystem dequeue from send queue to dispatch", NetworkPacketLogEntry.Create(-1, queueItem.Writer.GetUnsafePtr(), queueItem.Writer.Length, true));
                }
                #endif
                // END PING TRACE

                for (var hookIdx = 0; hookIdx < m_Hooks.Count; ++hookIdx)
                {
                    m_Hooks[hookIdx].OnBeforeSendBatch(clientId, queueItem.BatchHeader.BatchSize, queueItem.Writer.Length, queueItem.NetworkDelivery);
                }

                queueItem.Writer.Seek(0);
                #if UNITY_EDITOR || DEVELOPMENT_BUILD
                // Skipping the Verify and sneaking the write mark in because we know it's fine.
                queueItem.Writer.Handle->AllowedWriteMark = 2;
                #endif
                queueItem.Writer.WriteValue(queueItem.BatchHeader);

                try
                {
                    m_MessageSender.Send(clientId, queueItem.NetworkDelivery, queueItem.Writer);

                    for (var hookIdx = 0; hookIdx < m_Hooks.Count; ++hookIdx)
                    {
                        m_Hooks[hookIdx].OnAfterSendBatch(clientId, queueItem.BatchHeader.BatchSize, queueItem.Writer.Length, queueItem.NetworkDelivery);
                    }
                }
                finally
                {
                    queueItem.Writer.Dispose();
                }
            }
            sendQueueItem.Clear();
        }
    }

    private struct ReceiveQueueItem
    {
        public FastBufferReader Reader;
        public MessageHeader    Header;
        public ulong            SenderId;
        public float            Timestamp;
        public int              MessageHeaderSerializedSize;
    }

    private struct SendQueueItem
    {
        public          BatchHeader      BatchHeader;
        public          FastBufferWriter Writer;
        public readonly NetworkDelivery  NetworkDelivery;

        public SendQueueItem(NetworkDelivery delivery, int writerSize, Allocator writerAllocator, int maxWriterSize = -1)
        {
            Writer = new FastBufferWriter(writerSize, writerAllocator, maxWriterSize);
            NetworkDelivery = delivery;
            BatchHeader = default;
        }
    }

    // KEEPSAKE FIX made public
    public struct MessageWithHandler
    {
        public Type           MessageType;
        public MessageHandler Handler;
    }

    private struct PointerListWrapper<T> : IReadOnlyList<T> where T : unmanaged
    {
        private readonly unsafe T* m_Value;

        internal unsafe PointerListWrapper(T* ptr, int length)
        {
            m_Value = ptr;
            Count = length;
        }

        public int Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
        }

        public unsafe T this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => m_Value[index];
        }

        public IEnumerator<T> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    #if WITH_PING_TRACE
    private void OnNetworkTracerFoundIncomingQueued(NetworkTracer.TargetType type, IntPtr bytes, int offset, int length, int index)
    {
        unsafe
        {
            if (type is NetworkTracer.TargetType.RPCPing or NetworkTracer.TargetType.RPCPong && length > index + 16 && UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 10) == 'i' && UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 15) == 'b')
            {
                var pingId = (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 14) << 24) | (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 13) << 16) | (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 12) << 8) | (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 11) << 0);
                var profile = UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 16) == 0x01;
                if (profile)
                {
                    if (type == NetworkTracer.TargetType.RPCPing)
                    {
                        LogGlobal.InfoStructured(pingId, "< Netcode MessagingSystem handling incoming batch data containing RPCPing, adding to m_IncomingMessageQueue", StructuralTimeSampleLogEntry.Now());
                        m_CurrentLatencyTrace = pingId;
                        NetworkManager.Singleton.NetworkConfig.NetworkTransport.SetLatencyTraceId(pingId);
                    }
                    else
                    {
                        LogGlobal.InfoStructured(pingId, "< Netcode MessagingSystem handling incoming batch data containing RPCPong, adding to m_IncomingMessageQueue", StructuralTimeSampleLogEntry.Now());
                    }
                }
            }
            else if (type == NetworkTracer.TargetType.RPCLatencyTraceComplete)
            {
                NetworkManager.Singleton.NetworkConfig.NetworkTransport.SetLatencyTraceId(-1);
            }
        }
    }

    private void OnNetworkTracerFoundIncomingSendToHandler(NetworkTracer.TargetType type, IntPtr bytes, int offset, int length, int index)
    {
        unsafe
        {
            if (type is NetworkTracer.TargetType.RPCPing or NetworkTracer.TargetType.RPCPong && length > index + 16 && UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 10) == 'i' && UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 15) == 'b')
            {
                var pingId = (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 14) << 24) | (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 13) << 16) | (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 12) << 8) | (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 11) << 0);
                var profile = UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 16) == 0x01;
                if (profile)
                {
                    if (type == NetworkTracer.TargetType.RPCPing)
                    {
                        LogGlobal.InfoStructured(pingId, "< Netcode MessagingSystem handling message RPCPing, sending to handler", StructuralTimeSampleLogEntry.Now());
                    }
                    else
                    {
                        LogGlobal.InfoStructured(pingId, "< Netcode MessagingSystem handling message RPCPong, sending to handler", StructuralTimeSampleLogEntry.Now());
                    }
                }
            }
        }
    }

    private void OnNetworkTracerFoundOutgoingQueued(NetworkTracer.TargetType type, IntPtr bytes, int offset, int length, int index)
    {
        unsafe
        {
            if (type is NetworkTracer.TargetType.RPCPing or NetworkTracer.TargetType.RPCPong && length > index + 16 && UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 10) == 'i' && UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 15) == 'b')
            {
                var pingId = (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 14) << 24) | (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 13) << 16) | (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 12) << 8) | (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 11) << 0);
                var profile = UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 16) == 0x01;
                if (profile)
                {
                    if (type == NetworkTracer.TargetType.RPCPing)
                    {
                        LogGlobal.InfoStructured(pingId, "> Netcode MessagingSystem queueing RPCPing to send queue for client", StructuralTimeSampleLogEntry.Now());
                        m_CurrentLatencyTrace = pingId;
                        NetworkManager.Singleton.NetworkConfig.NetworkTransport.SetLatencyTraceId(pingId);
                    }
                    else
                    {
                        LogGlobal.InfoStructured(pingId, "> Netcode MessagingSystem queueing RPCPong to send queue for client", StructuralTimeSampleLogEntry.Now());
                    }
                }
            }
            else if (type == NetworkTracer.TargetType.RPCLatencyTraceComplete)
            {
                m_CurrentLatencyTrace = null;
                NetworkManager.Singleton.NetworkConfig.NetworkTransport.SetLatencyTraceId(-1);
            }
        }
    }

    private void OnNetworkTracerFoundOutgoingDispatched(NetworkTracer.TargetType type, IntPtr bytes, int offset, int length, int index)
    {
        unsafe
        {
            if (type is NetworkTracer.TargetType.RPCPing or NetworkTracer.TargetType.RPCPong && length > index + 16 && UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 10) == 'i' && UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 15) == 'b')
            {
                var pingId = (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 14) << 24) | (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 13) << 16) | (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 12) << 8) | (UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 11) << 0);
                var profile = UnsafeUtility.ArrayElementAsRef<byte>(bytes.ToPointer(), offset + index + 16) == 0x01;
                if (profile)
                {
                    if (type == NetworkTracer.TargetType.RPCPing)
                    {
                        LogGlobal.InfoStructured(pingId, "> Netcode MessagingSystem dequeuing RPCPing from send queue for client to pass to m_MessageSender", StructuralTimeSampleLogEntry.Now());
                    }
                    else
                    {
                        LogGlobal.InfoStructured(pingId, "> Netcode MessagingSystem dequeuing RPCPong from send queue for client to pass to m_MessageSender", StructuralTimeSampleLogEntry.Now());
                    }
                }
            }
        }
    }
    #endif
}

}
