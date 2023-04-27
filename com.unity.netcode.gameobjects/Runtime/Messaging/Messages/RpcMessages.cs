using System;
using UnityEngine;
using Unity.Collections;

namespace Unity.Netcode
{
    internal static class RpcMessageHelpers
    {
        public static unsafe void Serialize(ref FastBufferWriter writer, ref RpcMetadata metadata, ref FastBufferWriter payload)
        {
            if (!writer.TryBeginWrite(FastBufferWriter.GetWriteSize<RpcMetadata>() + payload.Length))
            {
                throw new OverflowException("Not enough space in the buffer to store RPC data.");
            }

            writer.WriteValue(metadata);
            writer.WriteBytes(payload.GetUnsafePtr(), payload.Length);
        }

        public static unsafe bool Deserialize(ref FastBufferReader reader, ref NetworkContext context, ref RpcMetadata metadata, ref FastBufferReader payload)
        {
            int metadataSize = FastBufferWriter.GetWriteSize<RpcMetadata>();
            if (!reader.TryBeginRead(metadataSize))
            {
                throw new InvalidOperationException("Not enough data in the buffer to read RPC meta.");
            }

            reader.ReadValue(out metadata);

            var networkManager = (NetworkManager)context.SystemOwner;
            // KEEPSAKE FIX - find objects in Attached instead of Spawned collection
            if (!networkManager.SpawnManager.AttachedObjects.ContainsKey(metadata.NetworkObjectId))
            {
                networkManager.SpawnManager.TriggerOnSpawn(metadata.NetworkObjectId, reader, ref context);
                return false;
            }

            // KEEPSAKE FIX - find objects in Attached instead of Spawned collection
            var networkObject = networkManager.SpawnManager.AttachedObjects[metadata.NetworkObjectId];
            string networkBehaviourName;
            var networkBehaviour = networkManager.SpawnManager.AttachedObjects[metadata.NetworkObjectId].GetNetworkBehaviourAtOrderIndex(metadata
            .NetworkBehaviourId);
            if (networkBehaviour != null)
            {
                networkBehaviourName = networkBehaviour.__getTypeName();
            }
            else
            {
                // KEEPSAKE FIX - added INetworkRpcHandler
                var networkRpcHandler = networkManager.SpawnManager.AttachedObjects[metadata.NetworkObjectId]
                                                      .GetRpcHandlerAtOrderIndex(metadata.NetworkBehaviourId);

                if (networkRpcHandler != null)
                {
                    networkBehaviourName = networkRpcHandler.__getTypeName();
                }
                else
                {
                    // neither NetworkBehaviour or INetworkRpcHandler found at index
                    return false;
                }
            }

            if (!NetworkManager.__rpc_func_table.ContainsKey(metadata.NetworkRpcMethodId))
            {
                return false;
            }

            payload = new FastBufferReader(reader.GetUnsafePtr() + metadataSize, Allocator.None, reader.Length - metadataSize);

#if DEVELOPMENT_BUILD || UNITY_EDITOR
            if (NetworkManager.__rpc_name_table.TryGetValue(metadata.NetworkRpcMethodId, out var rpcMethodName))
            {
                networkManager.NetworkMetrics.TrackRpcReceived(
                    context.SenderId,
                    networkObject,
                    rpcMethodName,
                    networkBehaviourName,
                    reader.Length);
            }
#endif

            return true;
        }

        public static void Handle(ref NetworkContext context, ref RpcMetadata metadata, ref FastBufferReader payload, ref __RpcParams rpcParams)
        {
            var networkManager = (NetworkManager)context.SystemOwner;
            // KEEPSAKE FIX - find objects in Attached instead of Spawned collection
            if (!networkManager.SpawnManager.AttachedObjects.TryGetValue(metadata.NetworkObjectId, out var networkObject))
            {
                throw new Exception($"Cannot call RPC on NetworkObject #{metadata.NetworkObjectId} because it is not spawned with the network layer, so it cannot be found. Or is it possible the object was despawned? Either that or its too early in its lifetime to call this RPC now, and you should wait for NetworkSetup or SafeStart.");
                //throw new Exception($"RPC called on a {nameof(NetworkObject)} that is not in the attached objects list - please make sure the {nameof(NetworkObject)} is attached before calling RPCs.");
            }
            var networkBehaviour = networkObject.GetNetworkBehaviourAtOrderIndex(metadata.NetworkBehaviourId);

            if (networkBehaviour != null)
            {
                try
                {
                    NetworkManager.__rpc_func_table[metadata.NetworkRpcMethodId](networkBehaviour, payload, rpcParams);
                }
                catch (Exception ex)
                {
                    Debug.LogException(new Exception("Unhandled RPC exception!", ex));
                }
            }
            else // KEEPSAKE FIX - INetworkRpcHandler
            {
                var networkRpcHandler = networkObject.GetRpcHandlerAtOrderIndex(metadata.NetworkBehaviourId);

                try
                {
                    NetworkManager.__rpc_func_table[metadata.NetworkRpcMethodId](networkRpcHandler, payload, rpcParams);
                }
                catch (Exception ex)
                {
                    Debug.LogException(new Exception("Unhandled RPC exception!", ex));
                }
            }
        }
    }

    internal struct RpcMetadata
    {
        public ulong NetworkObjectId;
        public ushort NetworkBehaviourId;
        public uint NetworkRpcMethodId;
    }

    internal struct ServerRpcMessage : INetworkMessage
    {
        public RpcMetadata Metadata;

        public FastBufferWriter WriteBuffer;
        public FastBufferReader ReadBuffer;

        public unsafe void Serialize(FastBufferWriter writer)
        {
            RpcMessageHelpers.Serialize(ref writer, ref Metadata, ref WriteBuffer);
        }

        public unsafe bool Deserialize(FastBufferReader reader, ref NetworkContext context)
        {
            return RpcMessageHelpers.Deserialize(ref reader, ref context, ref Metadata, ref ReadBuffer);
        }

        public void Handle(ref NetworkContext context)
        {
            var rpcParams = new __RpcParams
            {
                Server = new ServerRpcParams
                {
                    Receive = new ServerRpcReceiveParams
                    {
                        SenderClientId = context.SenderId,
                        IsPredicting = context.IsPredicting, // KEEPSAKE FIX
                    }
                }
            };
            RpcMessageHelpers.Handle(ref context, ref Metadata, ref ReadBuffer, ref rpcParams);
        }
    }

    internal struct ClientRpcMessage : INetworkMessage
    {
        public RpcMetadata Metadata;

        public FastBufferWriter WriteBuffer;
        public FastBufferReader ReadBuffer;

        public void Serialize(FastBufferWriter writer)
        {
            RpcMessageHelpers.Serialize(ref writer, ref Metadata, ref WriteBuffer);
        }

        public bool Deserialize(FastBufferReader reader, ref NetworkContext context)
        {
            return RpcMessageHelpers.Deserialize(ref reader, ref context, ref Metadata, ref ReadBuffer);
        }

        public void Handle(ref NetworkContext context)
        {
            var rpcParams = new __RpcParams
            {
                Client = new ClientRpcParams
                {
                    Receive = new ClientRpcReceiveParams
                    {
                    }
                }
            };
            RpcMessageHelpers.Handle(ref context, ref Metadata, ref ReadBuffer, ref rpcParams);
        }
    }
}
