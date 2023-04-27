using Cysharp.Threading.Tasks;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;

namespace Unity.Netcode
{
    internal struct CreateObjectMessage : INetworkMessage
    {
        public NetworkObject.SceneObject ObjectInfo;
        private NativeArray<byte> m_ReceivedNetworkVariableData;

        public void Serialize(FastBufferWriter writer)
        {
            ObjectInfo.Serialize(writer);
        }

        public bool Deserialize(FastBufferReader reader, ref NetworkContext context)
        {
            var networkManager = (NetworkManager)context.SystemOwner;
            if (!networkManager.IsClient)
            {
                return false;
            }
            ObjectInfo.Deserialize(reader);

            m_ReceivedNetworkVariableData = new NativeArray<byte>(
                reader.Length - reader.Position,
                Allocator.Persistent,
                NativeArrayOptions.UninitializedMemory);
            unsafe
            {
                UnsafeUtility.MemCpy(
                    m_ReceivedNetworkVariableData.GetUnsafePtr(),
                    reader.GetUnsafePtrAtCurrentPosition(),
                    m_ReceivedNetworkVariableData.Length);
            }

            return true;
        }

        public void Handle(ref NetworkContext context)
        {
            var networkManager = (NetworkManager)context.SystemOwner;
            HandleAsync(networkManager, context.SenderId, context.MessageSize, m_ReceivedNetworkVariableData, ObjectInfo).Forget();
        }

        private static async UniTask HandleAsync(
            NetworkManager networkManager,
            ulong senderId,
            long messageSize,
            NativeArray<byte> networkVariableData,
            NetworkObject.SceneObject sceneObject)
        {
            // need to be persistent since passing to async method
            using var reader = new FastBufferReader(networkVariableData, Allocator.Persistent);
            networkVariableData.Dispose(); // KEEPSAKE NOTE - dealloc now since using Allocator.Persistent for FastBufferReader above will copy and own the data

            var networkObject = await NetworkObject.AddSceneObjectAsync(sceneObject, reader, networkManager);
            networkManager.NetworkMetrics.TrackObjectSpawnReceived(senderId, networkObject, messageSize);
        }
    }
}
