using System.Collections.Generic;
using Unity.Profiling;

namespace Unity.Netcode
{
    public class NetworkBehaviourUpdater
    {
        private HashSet<NetworkObject> m_Touched = new HashSet<NetworkObject>();

        #if DEVELOPMENT_BUILD || UNITY_EDITOR
        private ProfilerMarker m_NetworkBehaviourUpdate = new ProfilerMarker($"{nameof(NetworkBehaviour)}.{nameof(NetworkBehaviourUpdate)}");
#endif

        internal void NetworkBehaviourUpdate(NetworkManager networkManager)
        {
#if DEVELOPMENT_BUILD || UNITY_EDITOR
            m_NetworkBehaviourUpdate.Begin();
#endif
            try
            {
                if (networkManager.IsServer)
                {
                    m_Touched.Clear();
                    var numClientsQueriedFor = 0;
                    for (int i = 0; i < networkManager.ConnectedClientsList.Count; i++)
                    {
                        var client = networkManager.ConnectedClientsList[i];
                        if (client.PlayerObject == null) // KEEPSAKE FIX
                        {
                            continue;
                        }

                        networkManager.InterestManager.QueryFor(ref client.PlayerObject, out var interestUpdateThisFrame);
                        numClientsQueriedFor++;
                        foreach (var sobj in interestUpdateThisFrame)
                        {
                            if (sobj.IsNetworkVisibleTo(client.ClientId))
                            {
                                m_Touched.Add(sobj);
                                // Sync just the variables for just the objects this client sees
                                var behaviours = sobj.ChildNetworkBehaviours_BehavioursOnly;
                                foreach (var networkBehaviour in behaviours)
                                {
                                    networkBehaviour.VariableUpdate(client.ClientId);
                                }
                            }
                        }
                    }

                    // Now, reset all the no-longer-dirty variables
                    foreach (var sobj in m_Touched)
                    {
                        foreach (var networkBehaviour in sobj.ChildNetworkBehaviours_BehavioursOnly)
                        {
                            networkBehaviour.PostNetworkVariableWrite();
                        }
                    }

                    // KEEPSAKE FIX - clear dirty flag when all known clients have checked their interest
                    if (numClientsQueriedFor == networkManager.ConnectedClientsList.Count)
                    {
                        networkManager.InterestManager.ClearInterestDirty();
                    }
                }
                else
                {
                    // when client updates the server, it tells it about all its objects
                    // KEEPSAKE FIX - find objects in Attached instead of Spawned collection
                    foreach (var sobj in networkManager.SpawnManager.AttachedObjectsList)
                    {
                        if (sobj.IsOwner)
                        {
                            foreach (var networkBehaviour in sobj.ChildNetworkBehaviours_BehavioursOnly)
                            {
                                networkBehaviour.VariableUpdate(networkManager.ServerClientId);
                            }
                        }
                    }

                    // Now, reset all the no-longer-dirty variables
                    // KEEPSAKE FIX - find objects in Attached instead of Spawned collection
                    foreach (var sobj in networkManager.SpawnManager.AttachedObjectsList)
                    {
                        foreach (var networkBehaviour in sobj.ChildNetworkBehaviours_BehavioursOnly)
                        {
                            networkBehaviour.PostNetworkVariableWrite();
                        }
                    }
                }
            }
            finally
            {
#if DEVELOPMENT_BUILD || UNITY_EDITOR
                m_NetworkBehaviourUpdate.End();
#endif
            }
        }

    }
}
