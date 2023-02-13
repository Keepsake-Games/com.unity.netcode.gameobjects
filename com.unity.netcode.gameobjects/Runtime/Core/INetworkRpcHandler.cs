using System;
using JetBrains.Annotations;
using Unity.Collections;
using UnityEngine;

// ReSharper disable once CheckNamespace
namespace Unity.Netcode
{
    // KEEPSAKE FIX - added this interface to add the RPC-handling part of NetworkBehaviour to other classes (NetworkObject_Base)
    public interface INetworkRpcHandler
    {
        #pragma warning disable IDE1006 // disable naming rule violation check
        // ReSharper disable once InconsistentNaming
        enum __RpcExecStage
            #pragma warning restore IDE1006 // restore naming rule violation check
        {
            None   = 0,
            Server = 1,
            Client = 2,
        }

        #pragma warning disable IDE1006 // disable naming rule violation check
        // NetworkBehaviourILPP will override this in derived classes to return the name of the concrete type
        protected internal string __getTypeName()
        {
            return nameof(INetworkRpcHandler);
        }
        #pragma warning restore IDE1006 // restore naming rule violation check

        private const int k_RpcMessageDefaultSize = 1024;      // 1k
        private const int k_RpcMessageMaximumSize = 1024 * 64; // 64k

        #pragma warning disable IDE1006 // disable naming rule violation check
        // ReSharper disable once UnusedMember.Global
        // ReSharper disable once InconsistentNaming
        public __RpcExecStage __rpc_exec_stage { get; set; }
        #pragma warning restore IDE1006 // restore naming rule violation check

        ushort NetworkBehaviourIdCache { get; set; }

        NetworkObject NetworkObject { get; }

        public NetworkManager NetworkManager => NetworkObject.NetworkManager;

        public ulong NetworkObjectId => NetworkObject.NetworkObjectId;

        public ushort NetworkBehaviourId => NetworkObject.GetNetworkBehaviourOrderIndex(this);

        /// <summary>
        /// Gets the ClientId that owns the NetworkObject
        /// </summary>
        [UsedImplicitly]
        public ulong OwnerClientId => NetworkObject.OwnerClientId;

        public bool IsServer => NetworkManager.IsServer;

        public bool IsClient => NetworkManager.IsClient;
        public bool IsHost   => NetworkManager.IsHost;

        #pragma warning disable IDE1006 // disable naming rule violation check
        [UsedImplicitly]
        public FastBufferWriter __beginSendServerRpc(uint rpcMethodId, ServerRpcParams serverRpcParams, RpcDelivery rpcDelivery)
        {
            return new FastBufferWriter(k_RpcMessageDefaultSize, Allocator.Temp, k_RpcMessageMaximumSize);
        }
        #pragma warning restore IDE1006 // restore naming rule violation check

        #pragma warning disable IDE1006 // disable naming rule violation check
        [UsedImplicitly]
        public void __endSendServerRpc(
                ref FastBufferWriter bufferWriter,
                uint rpcMethodId,
                ServerRpcParams serverRpcParams,
                RpcDelivery rpcDelivery, /* KEEPSAKE FIX */
                bool withLocalPrediction)
            #pragma warning restore IDE1006 // restore naming rule violation check
        {
            var serverRpcMessage = new ServerRpcMessage
            {
                Metadata = new RpcMetadata
                {
                    NetworkObjectId = NetworkObjectId,
                    NetworkBehaviourId = NetworkBehaviourId,
                    NetworkRpcMethodId = rpcMethodId,
                },
                WriteBuffer = bufferWriter,
            };

            NetworkDelivery networkDelivery;
            switch (rpcDelivery)
            {
                default:
                case RpcDelivery.Reliable:
                    networkDelivery = NetworkDelivery.ReliableFragmentedSequenced;
                    break;
                case RpcDelivery.Unreliable:
                    if (bufferWriter.Length > MessagingSystem.NON_FRAGMENTED_MESSAGE_MAX_SIZE)
                    {
                        throw new OverflowException("RPC parameters are too large for unreliable delivery.");
                    }
                    networkDelivery = NetworkDelivery.Unreliable;
                    break;
            }

            int rpcWriteSize;

            // If we are a server/host then we just no op and send to ourself
            if (IsHost || IsServer)
            {
                using var tempBuffer = new FastBufferReader(bufferWriter, Allocator.Temp);
                var context = new NetworkContext
                {
                    SenderId = NetworkManager.ServerClientId,
                    Timestamp = Time.realtimeSinceStartup,
                    SystemOwner = NetworkManager,
                    // header information isn't valid since it's not a real message.
                    // RpcMessage doesn't access this stuff so it's just left empty.
                    Header = new MessageHeader(),
                    SerializedHeaderSize = 0,
                    MessageSize = 0,
                };
                serverRpcMessage.ReadBuffer = tempBuffer;
                serverRpcMessage.Handle(ref context);
                rpcWriteSize = tempBuffer.Length;
            }
            else
            {
                rpcWriteSize = NetworkManager.SendMessage(ref serverRpcMessage, networkDelivery, NetworkManager.ServerClientId);

                // KEEPSAKE FIX - local prediction
                if (withLocalPrediction)
                {
                    using var tempBuffer = new FastBufferReader(bufferWriter, Allocator.Temp);
                    var context = new NetworkContext
                    {
                        IsPredicting = true,
                        SenderId = NetworkManager.LocalClientId,
                        Timestamp = Time.realtimeSinceStartup,
                        SystemOwner = NetworkManager,
                        // header information isn't valid since it's not a real message.
                        // RpcMessage doesn't access this stuff so it's just left empty.
                        Header = new MessageHeader(),
                        SerializedHeaderSize = 0,
                        MessageSize = 0,
                    };
                    serverRpcMessage.ReadBuffer = tempBuffer;
                    serverRpcMessage.Handle(ref context);
                }
                // END KEEPSAKE FIX
            }

            bufferWriter.Dispose();

            #if DEVELOPMENT_BUILD || UNITY_EDITOR
            if (NetworkManager.__rpc_name_table.TryGetValue(rpcMethodId, out var rpcMethodName))
            {
                NetworkManager.NetworkMetrics.TrackRpcSent(
                    NetworkManager.ServerClientId,
                    NetworkObject,
                    rpcMethodName,
                    __getTypeName(),
                    rpcWriteSize);
            }
            #endif
        }

        #pragma warning disable IDE1006 // disable naming rule violation check
        [UsedImplicitly]
        public FastBufferWriter __beginSendClientRpc(uint rpcMethodId, ClientRpcParams clientRpcParams, RpcDelivery rpcDelivery)
            #pragma warning restore IDE1006 // restore naming rule violation check
        {
            return new FastBufferWriter(k_RpcMessageDefaultSize, Allocator.Temp, k_RpcMessageMaximumSize);
        }

        #pragma warning disable IDE1006 // disable naming rule violation check
        [UsedImplicitly]
        public void __endSendClientRpc(
                ref FastBufferWriter bufferWriter,
                uint rpcMethodId,
                ClientRpcParams clientRpcParams,
                RpcDelivery rpcDelivery)
            #pragma warning restore IDE1006 // restore naming rule violation check
        {
            var clientRpcMessage = new ClientRpcMessage
            {
                Metadata = new RpcMetadata
                {
                    NetworkObjectId = NetworkObjectId,
                    NetworkBehaviourId = NetworkBehaviourId,
                    NetworkRpcMethodId = rpcMethodId,
                },
                WriteBuffer = bufferWriter,
            };

            NetworkDelivery networkDelivery;
            switch (rpcDelivery)
            {
                default:
                case RpcDelivery.Reliable:
                    networkDelivery = NetworkDelivery.ReliableFragmentedSequenced;
                    break;
                case RpcDelivery.Unreliable:
                    if (bufferWriter.Length > MessagingSystem.NON_FRAGMENTED_MESSAGE_MAX_SIZE)
                    {
                        throw new OverflowException("RPC parameters are too large for unreliable delivery.");
                    }
                    networkDelivery = NetworkDelivery.Unreliable;
                    break;
            }

            int rpcWriteSize;

            // We check to see if we need to shortcut for the case where we are the host/server and we can send a clientRPC
            // to ourself. Sadly we have to figure that out from the list of clientIds :(
            var shouldSendToHost = false;

            if (clientRpcParams.Send.TargetClientIds != null)
            {
                foreach (var clientId in clientRpcParams.Send.TargetClientIds)
                {
                    if (clientId == NetworkManager.ServerClientId)
                    {
                        shouldSendToHost = true;
                        break;
                    }
                }

                rpcWriteSize = NetworkManager.SendMessage(ref clientRpcMessage, networkDelivery, in clientRpcParams.Send.TargetClientIds);
            }
            else if (clientRpcParams.Send.TargetClientIdsNativeArray != null)
            {
                foreach (var clientId in clientRpcParams.Send.TargetClientIdsNativeArray)
                {
                    if (clientId == NetworkManager.ServerClientId)
                    {
                        shouldSendToHost = true;
                        break;
                    }
                }

                rpcWriteSize = NetworkManager.SendMessage(
                    ref clientRpcMessage,
                    networkDelivery,
                    clientRpcParams.Send.TargetClientIdsNativeArray.Value);
            }
            else
            {
                shouldSendToHost = IsHost;
                rpcWriteSize = NetworkManager.SendMessage(ref clientRpcMessage, networkDelivery, NetworkManager.ConnectedClientsIds);
            }

            // If we are a server/host then we just no op and send to ourself
            if (shouldSendToHost)
            {
                using var tempBuffer = new FastBufferReader(bufferWriter, Allocator.Temp);
                var context = new NetworkContext
                {
                    SenderId = NetworkManager.ServerClientId,
                    Timestamp = Time.realtimeSinceStartup,
                    SystemOwner = NetworkManager,
                    // header information isn't valid since it's not a real message.
                    // RpcMessage doesn't access this stuff so it's just left empty.
                    Header = new MessageHeader(),
                    SerializedHeaderSize = 0,
                    MessageSize = 0,
                };
                clientRpcMessage.ReadBuffer = tempBuffer;
                clientRpcMessage.Handle(ref context);
            }

            bufferWriter.Dispose();

            #if DEVELOPMENT_BUILD || UNITY_EDITOR
            if (NetworkManager.__rpc_name_table.TryGetValue(rpcMethodId, out var rpcMethodName))
            {
                foreach (var client in NetworkManager.ConnectedClients)
                {
                    NetworkManager.NetworkMetrics.TrackRpcSent(client.Key, NetworkObject, rpcMethodName, __getTypeName(), rpcWriteSize);
                }
            }
            #endif
        }
    }
}
