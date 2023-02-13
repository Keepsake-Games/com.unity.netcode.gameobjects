using System;
using System.Collections.Generic;
using Cysharp.Threading.Tasks;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;

namespace Unity.Netcode
{
    internal struct ConnectionApprovedMessage : INetworkMessage
    {
        public ulong OwnerClientId;
        public int NetworkTick;

        // KEEPSAKE FIX
        public bool SyncOnlyPlayerObjects;

        // Not serialized, held as references to serialize NetworkVariable data
        public HashSet<NetworkObject> SpawnedObjectsList;

        private NativeArray<byte> m_ReceivedSceneObjectData;

        public void Serialize(FastBufferWriter writer)
        {
            if (!writer.TryBeginWrite(sizeof(ulong) + sizeof(int) + sizeof(int)))
            {
                throw new OverflowException(
                    $"Not enough space in the write buffer to serialize {nameof(ConnectionApprovedMessage)}");
            }
            writer.WriteValue(OwnerClientId);
            writer.WriteValue(NetworkTick);

            uint sceneObjectCount = 0;
            if (SpawnedObjectsList != null)
            {
                var pos = writer.Position;
                writer.Seek(writer.Position + FastBufferWriter.GetWriteSize(sceneObjectCount));

                // Serialize NetworkVariable data
                foreach (var sobj in SpawnedObjectsList)
                {
                    if (sobj.CheckObjectVisibility == null || sobj.CheckObjectVisibility(OwnerClientId))
                    {
                        if (SyncOnlyPlayerObjects && !sobj.IsPlayerObject)
                        {
                            continue;
                        }

                        sobj.Observers.Add(OwnerClientId);
                        var sceneObject = sobj.GetMessageSceneObject(OwnerClientId);
                        sceneObject.Serialize(writer);

                        #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
                        writer.WriteValueSafe("end_scene_object", true);
                        #endif

                        UnityEngine.Debug.Log($"ConnectionApprovedMessage scene object with hash {sobj.GlobalObjectIdHash} -- {sobj}");
                        ++sceneObjectCount;
                    }
                }
                writer.Seek(pos);
                writer.WriteValue(sceneObjectCount);
                writer.Seek(writer.Length);
            }
            else
            {
                writer.WriteValue(sceneObjectCount);
            }
        }

        public bool Deserialize(FastBufferReader reader, ref NetworkContext context)
        {
            var networkManager = (NetworkManager)context.SystemOwner;
            if (!networkManager.IsClient)
            {
                return false;
            }

            if (!reader.TryBeginRead(sizeof(ulong) + sizeof(int) + sizeof(int)))
            {
                throw new OverflowException(
                    $"Not enough space in the buffer to read {nameof(ConnectionApprovedMessage)}");
            }

            reader.ReadValue(out OwnerClientId);
            reader.ReadValue(out NetworkTick);
            m_ReceivedSceneObjectData = new NativeArray<byte>(
                reader.Length - reader.Position,
                Allocator.Persistent,
                NativeArrayOptions.UninitializedMemory);
            unsafe
            {
                UnsafeUtility.MemCpy(
                    m_ReceivedSceneObjectData.GetUnsafePtr(),
                    reader.GetUnsafePtrAtCurrentPosition(),
                    m_ReceivedSceneObjectData.Length);
            }
            return true;
        }

        public void Handle(ref NetworkContext context)
        {
            var networkManager = (NetworkManager)context.SystemOwner;
            networkManager.LocalClientId = OwnerClientId;
            networkManager.NetworkMetrics.SetConnectionId(networkManager.LocalClientId);

            var time = new NetworkTime(networkManager.NetworkTickSystem.TickRate, NetworkTick);
            networkManager.NetworkTimeSystem.Reset(time.Time, 0.15f); // Start with a constant RTT of 150 until we receive values from the transport.
            networkManager.NetworkTickSystem.Reset(networkManager.NetworkTimeSystem.LocalTime, networkManager.NetworkTimeSystem.ServerTime);

            networkManager.LocalClient = new NetworkClient() { ClientId = networkManager.LocalClientId };

            // Only if scene management is disabled do we handle NetworkObject synchronization at this point
            // KEEPSAKE FIX - use our more granular sync settings
            if (networkManager.NetworkConfig.SyncAllNetworkObjectsInConnectionApprovedMessage || networkManager.NetworkConfig.SyncPlayerObjectsInConnectionApprovedMessage)
            {
                // KEEPSAKE FIX - Don't call this, it destroys our LegacyNetwork manager which is placed in Global and loaded at this point, should Netcode just hook it up rather than destroying?
                //                Its possible we'll need to re-enable this later when we know more
                //networkManager.SpawnManager.DestroySceneObjects();

                HandleAsync(context.SenderId, m_ReceivedSceneObjectData, networkManager).Forget();
            }
        }

        private static async UniTask HandleAsync(ulong senderId, NativeArray<byte> allSceneObjectsData, NetworkManager networkManager)
        {
            try
            {
                // We used to use Allocator.None to not have to copy the byte data (we control its lifetime anyway) BUT that didn't work
                // since FastBufferReader will allocate some internals in Temp which has a sub-frame lifetime and thus incompatible with async/await
                using var reader = new FastBufferReader(allSceneObjectsData, Allocator.Persistent);
                reader.ReadValueSafe(out uint sceneObjectCount);

                // Deserializing NetworkVariable data is deferred from Receive() to Handle to avoid needing
                // to create a list to hold the data. This is a breach of convention for performance reasons.
                for (ushort i = 0; i < sceneObjectCount; i++)
                {
                    var sceneObject = new NetworkObject.SceneObject();
                    sceneObject.Deserialize(reader);
                    await NetworkObject.AddSceneObjectAsync(sceneObject, reader, networkManager);

                    #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
                    reader.ReadValueSafe(out var marker, true);
                    UnityEngine.Debug.Assert(marker == "end_scene_object", "scene object in packet ends with correct marker");
                    #endif
                }

                // Mark the client being connected
                networkManager.IsConnectedClient = true;
                // When scene management is disabled we notify after everything is synchronized
                networkManager.InvokeOnClientConnectedCallback(senderId);
            }
            finally
            {
                if (allSceneObjectsData.IsCreated)
                {
                    allSceneObjectsData.Dispose();
                }
            }
        }
    }
}
