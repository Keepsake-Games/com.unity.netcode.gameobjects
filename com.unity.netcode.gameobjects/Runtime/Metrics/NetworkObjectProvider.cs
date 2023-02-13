#if MULTIPLAYER_TOOLS
using Unity.Multiplayer.Tools;
using UnityEngine;

namespace Unity.Netcode
{
    class NetworkObjectProvider : INetworkObjectProvider
    {
        private readonly NetworkManager m_NetworkManager;

        public NetworkObjectProvider(NetworkManager networkManager)
        {
            m_NetworkManager = networkManager;
        }

        public Object GetNetworkObject(ulong networkObjectId)
        {
            // KEEPSAKE FIX - find objects in Attached instead of Spawned collection
            if(m_NetworkManager.SpawnManager.AttachedObjects.TryGetValue(networkObjectId, out NetworkObject value))
            {
                return value;
            }

            return null;
        }
    }
}
#endif
