using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cysharp.Threading.Tasks;
using Keepsake.Common;
using UniRx;
using Unity.Collections;
using UnityEngine;
using UnityEngine.AddressableAssets;
using UnityEngine.Assertions;
using Object = UnityEngine.Object;

namespace Unity.Netcode
{

/// <summary>
///     Class that handles object spawning
/// </summary>
public class NetworkSpawnManager
{
    // KEEPSAKE FIX
    /// <summary>
    ///     The currently attached (tracked) objects
    /// </summary>
    public readonly Dictionary<ulong, NetworkObject> AttachedObjects = new();

    /// <summary>
    ///     A list of the attached (tracked) objects
    /// </summary>
    public readonly HashSet<NetworkObject> AttachedObjectsList = new();
    // KEEPSAKE FIX
    // Spawned = Default Netcode concept where Netcode controls lifetime and does tracking of changed Network Variables on the object. (Lifetime control + Tracked)
    // Tracked = Our concept where lifetime is not controlled by Netcode, but it tracks Network Variables on the object.
    // Attach = The act of "start tracking"
    // Detach = The act of "stop tracking", leaving the objects as they where ("detaching" them from networking)\

    // KEEPSAKE FIX
    public IObservable<NetworkObject> WhenObjectAttached => m_ObjectAttachedSubject;
    public IObservable<NetworkObject> WhenObjectDespawned => m_ObjectDespawnedSubject;

    /// <summary>
    ///     The currently spawned objects
    /// </summary>
    public readonly Dictionary<ulong, NetworkObject> SpawnedObjects = new();

    /// <summary>
    ///     A list of the spawned objects
    /// </summary>
    public readonly HashSet<NetworkObject> SpawnedObjectsList = new();

    internal readonly Queue<ReleasedNetworkId> ReleasedNetworkObjectIds = new();
    private           ulong                    m_NetworkObjectIdCounter;

    /// <summary>
    /// KEEPSAKE FIX - nesting
    /// Used to track nested NetworkObjects
    /// We store them by:
    /// [GlobalObjectIdHash][SpawnedParentObjectId][NetworkObject]
    /// The SpawnedParentObjectId aspect allows us to distinguish multiple instance of a prefab having the same nested objects (which have the same GlobalObjectIdHash).
    /// This collection contains the nested objects not yet spawned.
    /// </summary>
    internal readonly Dictionary<uint, Dictionary<ulong, NetworkObject>> NestedObjects = new();

    // A list of target ClientId, use when sending despawn commands. Kept as a member to reduce memory allocations
    private List<ulong> m_TargetClientIds = new();

    private readonly Dictionary<ulong, TriggerInfo> m_Triggers = new();

    // KEEPSAKE FIX
    internal readonly Subject<NetworkObject> m_ObjectAttachedSubject = new();
    internal readonly Subject<NetworkObject> m_ObjectDespawnedSubject = new();

    /// <summary>
    ///     Gets the NetworkManager associated with this SpawnManager.
    /// </summary>
    public NetworkManager NetworkManager { get; }

    internal NetworkSpawnManager(NetworkManager networkManager)
    {
        NetworkManager = networkManager;
    }

    private const char k_PathPartsSeparator = '/';

    internal ulong GetNetworkObjectId()
    {
        if (ReleasedNetworkObjectIds.Count > 0 && NetworkManager.NetworkConfig.RecycleNetworkIds && Time.unscaledTime - ReleasedNetworkObjectIds.Peek().ReleaseTime >= NetworkManager.NetworkConfig.NetworkIdRecycleDelay)
        {
            return ReleasedNetworkObjectIds.Dequeue().NetworkId;
        }

        // KEEPSAKE FIX - LegacyNetwork's NetworkViewId has been tucked into the higher half of ulong so never go there with Netcode.
        //                This will be removed and Netcode will get all its bits once we've moved away from maintaining 2 sets
        //                of network ids (LegacyNetwork and Netcode)
        if (m_NetworkObjectIdCounter >= (ulong)1 << 32)
        {
            LogNetcode.Error("Netcode ran out of NetworkObject Ids! Too many NetworkObjects, but the capacity is so high that there's probably a bug where NetworkObjects are leaked.. VERY BAD");
            throw new Exception("Netcode ran out of NetworkObject Ids! Too many NetworkObjects, but the capacity is so high that there's probably a bug where NetworkObjects are leaked.. VERY BAD");
        }

        m_NetworkObjectIdCounter++;

        // KEEPSAKE FIX - support a "null" network object ID which should never be used
        if (m_NetworkObjectIdCounter == NetworkObjectReference.NullNetworkObjectId)
        {
            m_NetworkObjectIdCounter++;
        }

        return m_NetworkObjectIdCounter;
    }

    /// <summary>
    ///     Returns the local player object or null if one does not exist
    /// </summary>
    /// <returns>The local player object or null if one does not exist</returns>
    public NetworkObject GetLocalPlayerObject()
    {
        return GetPlayerNetworkObject(NetworkManager.LocalClientId);
    }

    /// <summary>
    ///     Returns the player object with a given clientId or null if one does not exist. This is only valid server side.
    /// </summary>
    /// <returns>The player object with a given clientId or null if one does not exist</returns>
    public NetworkObject GetPlayerNetworkObject(ulong clientId)
    {
        if (!NetworkManager.IsServer && NetworkManager.LocalClientId != clientId)
        {
            throw new NotServerException("Only the server can find player objects from other clients.");
        }

        if (TryGetNetworkClient(clientId, out var networkClient))
        {
            return networkClient.PlayerObject;
        }

        return null;
    }

    /// <summary>
    ///     Defers processing of a message until the moment a specific networkObjectId is spawned.
    ///     This is to handle situations where an RPC or other object-specific message arrives before the spawn does,
    ///     either due to it being requested in OnNetworkSpawn before the spawn call has been executed, or with
    ///     snapshot spawns enabled where the spawn is sent unreliably and not until the end of the frame.
    ///     There is a one second maximum lifetime of triggers to avoid memory leaks. After one second has passed
    ///     without the requested object ID being spawned, the triggers for it are automatically deleted.
    /// </summary>
    internal unsafe void TriggerOnSpawn(ulong networkObjectId, FastBufferReader reader, ref NetworkContext context)
    {
        if (!m_Triggers.ContainsKey(networkObjectId))
        {
            m_Triggers[networkObjectId] = new TriggerInfo
            {
                //Expiry = Time.realtimeSinceStartup + 1,
                Expiry = Time.realtimeSinceStartup + 120, // KEEPSAKE FIX - let trigger live much longer to support our "snapshot groups" deferring spawning etc
                TriggerData = new NativeList<TriggerData>(Allocator.Persistent),
            };
        }

        m_Triggers[networkObjectId].TriggerData.Add(
            new TriggerData
            {
                Reader = new FastBufferReader(reader.GetUnsafePtr(), Allocator.Persistent, reader.Length),
                Header = context.Header,
                Timestamp = context.Timestamp,
                SenderId = context.SenderId,
                SerializedHeaderSize = context.SerializedHeaderSize,
            });
    }

    /// <summary>
    ///     Cleans up any trigger that's existed for more than a second.
    ///     These triggers were probably for situations where a request was received after a despawn rather than before a
    ///     spawn.
    /// </summary>
    internal unsafe void CleanupStaleTriggers()
    {
        var staleKeys = stackalloc ulong[m_Triggers.Count()];
        var index = 0;
        foreach (var kvp in m_Triggers)
        {
            if (kvp.Value.Expiry < Time.realtimeSinceStartup)
            {
                staleKeys[index++] = kvp.Key;
                if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
                {
                    NetworkLog.LogWarning($"Deferred messages were received for {nameof(NetworkObject)} #{kvp.Key}, but it did not spawn until trigger expired.");
                }

                foreach (var data in kvp.Value.TriggerData)
                {
                    data.Reader.Dispose();
                }

                kvp.Value.TriggerData.Dispose();
            }
        }

        for (var i = 0; i < index; ++i)
        {
            m_Triggers.Remove(staleKeys[i]);
        }
    }

    /// <summary>
    ///     Cleans up any trigger that's existed for more than a second.
    ///     These triggers were probably for situations where a request was received after a despawn rather than before a
    ///     spawn.
    /// </summary>
    internal void CleanupAllTriggers()
    {
        foreach (var kvp in m_Triggers)
        {
            foreach (var data in kvp.Value.TriggerData)
            {
                data.Reader.Dispose();
            }

            kvp.Value.TriggerData.Dispose();
        }

        m_Triggers.Clear();
    }

    internal void RemoveOwnership(NetworkObject networkObject)
    {
        if (!NetworkManager.IsServer)
        {
            throw new NotServerException("Only the server can change ownership");
        }

        // KEEPSAKE FIX - check IsAttached and not IsSpawned
        if (!networkObject.IsAttached)
        {
            throw new SpawnStateException("Object is not attached");
        }

        // If we made it here then we are the server and if the server is determined to already be the owner
        // then ignore the RemoveOwnership invocation.
        if (networkObject.OwnerClientId == NetworkManager.ServerClientId)
        {
            return;
        }

        // Make sure the connected client entry exists before trying to remove ownership.
        if (TryGetNetworkClient(networkObject.OwnerClientId, out var networkClient))
        {
            for (var i = networkClient.OwnedObjects.Count - 1; i > -1; i--)
            {
                if (networkClient.OwnedObjects[i] == networkObject)
                {
                    networkClient.OwnedObjects.RemoveAt(i);
                }
            }

            networkObject.OwnerClientIdInternal = null;

            var message = new ChangeOwnershipMessage
            {
                NetworkObjectId = networkObject.NetworkObjectId,
                OwnerClientId = networkObject.OwnerClientId,
            };
            var size = NetworkManager.SendMessage(ref message, NetworkDelivery.ReliableSequenced, NetworkManager.ConnectedClientsIds);

            foreach (var client in NetworkManager.ConnectedClients)
            {
                NetworkManager.NetworkMetrics.TrackOwnershipChangeSent(client.Key, networkObject, size);
            }
        }
        else
        {
            if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
            {
                NetworkLog.LogWarning($"No connected clients prior to removing ownership for {networkObject.name}.  Make sure you are not initializing or shutting down when removing ownership.");
            }
        }
    }

    /// <summary>
    ///     Helper function to get a network client for a clientId from the NetworkManager.
    ///     On the server this will check the <see cref="NetworkManager.ConnectedClients" /> list.
    ///     On a non-server this will check the <see cref="NetworkManager.LocalClient" /> only.
    /// </summary>
    /// <param name="clientId">The clientId for which to try getting the NetworkClient for.</param>
    /// <param name="networkClient">The found NetworkClient. Null if no client was found.</param>
    /// <returns>True if a NetworkClient with a matching id was found else false.</returns>
    private bool TryGetNetworkClient(ulong clientId, out NetworkClient networkClient)
    {
        if (NetworkManager.IsServer)
        {
            return NetworkManager.ConnectedClients.TryGetValue(clientId, out networkClient);
        }

        if (NetworkManager.LocalClient != null && clientId == NetworkManager.LocalClient.ClientId)
        {
            networkClient = NetworkManager.LocalClient;
            return true;
        }

        networkClient = null;
        return false;
    }

    internal void ChangeOwnership(NetworkObject networkObject, ulong clientId)
    {
        if (!NetworkManager.IsServer)
        {
            throw new NotServerException("Only the server can change ownership");
        }

        // KEEPSAKE FIX - check IsAttached and not IsSpawned
        if (!networkObject.IsAttached)
        {
            throw new SpawnStateException("Object is not attached");
        }

        // KEEPSAKE FIX - don't see a point triggering lost ownership and gained ownership callback so ignoring of "changing" to same
        if (networkObject.OwnerClientId == clientId)
        {
            return;
        }

        if (TryGetNetworkClient(networkObject.OwnerClientId, out var networkClient))
        {
            for (var i = networkClient.OwnedObjects.Count - 1; i >= 0; i--)
            {
                if (networkClient.OwnedObjects[i] == networkObject)
                {
                    networkClient.OwnedObjects.RemoveAt(i);
                }
            }

            networkClient.OwnedObjects.Add(networkObject);
        }

        // KEEPSAKE FIX - invoke lost ownership callback on servers too
        var ownerChange = networkObject.OwnerClientId != clientId;
        if (ownerChange && networkObject.OwnerClientId == NetworkManager.LocalClientId)
        {
            //We are current owner.
            networkObject.InvokeBehaviourOnLostOwnership();
        }
        // END KEEPSAKE FIX

        networkObject.OwnerClientId = clientId;

        // KEEPSAKE FIX - invoke gained ownership callback on servers too
        if (ownerChange && clientId == NetworkManager.LocalClientId)
        {
            //We are new owner.
            networkObject.InvokeBehaviourOnGainedOwnership();
        }
        // END KEEPSAKE FIX

        // KEEPSAKE FIX - invoke our custom ownership changed callback which is called for all peers (that know), not only old/new owner
        if (ownerChange)
        {
            networkObject.InvokeBehaviourOnOwnershipChanged(networkObject.OwnerClientId, clientId);
        }

        if (TryGetNetworkClient(clientId, out var newNetworkClient))
        {
            newNetworkClient.OwnedObjects.Add(networkObject);
        }

        var message = new ChangeOwnershipMessage
        {
            NetworkObjectId = networkObject.NetworkObjectId,
            OwnerClientId = networkObject.OwnerClientId,
        };
        var size = NetworkManager.SendMessage(ref message, NetworkDelivery.ReliableSequenced, NetworkManager.ConnectedClientsIds);

        foreach (var client in NetworkManager.ConnectedClients)
        {
            NetworkManager.NetworkMetrics.TrackOwnershipChangeSent(client.Key, networkObject, size);
        }
    }

    /// <summary>
    ///     Should only run on the client
    /// </summary>
    /// KEEPSAKE FIX - added spawnedParentId for nesting
    internal async UniTask<NetworkObject> CreateLocalNetworkObjectAsync(bool isSceneObject, uint globalObjectIdHash, ulong ownerClientId, ulong? parentNetworkId, ulong? spawnedParentId, bool waitForParentIfMissing, Vector3? position, Quaternion? rotation, bool isReparented = false)
    {
        NetworkObject parentNetworkObject = null;

        if (parentNetworkId != null && !isReparented)
        {
            // KEEPSAKE FIX - wait for parent, up to a point
            const float maxWaitTime = 30.0f;
            var startTime = Time.unscaledTime;
            var parentFound = false;

            do
            {
                parentFound = AttachedObjects.TryGetValue(parentNetworkId.Value, out parentNetworkObject);
                if (!parentFound)
                {
                    if (!waitForParentIfMissing || startTime + maxWaitTime < Time.unscaledTime)
                    {
                        break;
                    }

                    //Debug.Log($"Waiting for NO parent with ID {parentNetworkId.Value}");
                    await UniTask.DelayFrame(3);
                }
            } while (!parentFound);

            if (!parentFound)
            {
                if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
                {
                    NetworkLog.LogWarning($"Cannot find parent with ID {parentNetworkId.Value}. Parent objects always have to be spawned and replicated BEFORE the child");
                }
            }
            //Debug.Log($"Found NO parent with ID {parentNetworkId.Value}: {parentNetworkObject.gameObject}");
        }

        // KEEPSAKE FIX - we don't have SceneManagement enabled but we still want to have Scene Objects
        if (/*!NetworkManager.NetworkConfig.EnableSceneManagement || */!isSceneObject)
        {
            // KEEPSAKE FIX - nesting
            if (spawnedParentId.HasValue)
            {
                if (!NestedObjects.TryGetValue(globalObjectIdHash, out var spawnedParentToInstanceMap))
                {
                    NetworkLog.LogError($"Unknown nested object (hash={globalObjectIdHash}) tried to spawn. Maybe it's parent with NetworkObject ID {spawnedParentId.Value} hasn't spawned yet (and thus we haven't been made aware of its nested objects)");
                    return null;
                }

                if (!spawnedParentToInstanceMap.TryGetValue(spawnedParentId.Value, out var networkObject))
                {
                    NetworkLog.LogError($"No instance of nested object with hash {globalObjectIdHash} found in spawned parent with ID {spawnedParentId.Value}, could the parent have been despawned?");
                    return null;
                }

                // once resolved remove the tracking
                spawnedParentToInstanceMap.Remove(spawnedParentId.Value);
                if (spawnedParentToInstanceMap.Count == 0)
                {
                    NestedObjects.Remove(globalObjectIdHash);
                }

                return networkObject;
            }

            // If the prefab hash has a registered INetworkPrefabInstanceHandler derived class
            if (NetworkManager.PrefabHandler.ContainsHandler(globalObjectIdHash))
            {
                // Let the handler spawn the NetworkObject
                var networkObject = NetworkManager.PrefabHandler.HandleNetworkPrefabSpawn(globalObjectIdHash, ownerClientId, position.GetValueOrDefault(Vector3.zero), rotation.GetValueOrDefault(Quaternion.identity));

                networkObject.NetworkManagerOwner = NetworkManager;

                if (parentNetworkObject != null)
                {
                    networkObject.transform.SetParent(parentNetworkObject.transform, true);
                }

                if (NetworkSceneManager.IsSpawnedObjectsPendingInDontDestroyOnLoad)
                {
                    Object.DontDestroyOnLoad(networkObject.gameObject);
                }

                return networkObject;
            }
            else
            {
                // See if there is a valid registered NetworkPrefabOverrideLink associated with the provided prefabHash
                GameObject networkPrefabReference = null;
                if (NetworkManager.NetworkConfig.NetworkPrefabOverrideLinks.TryGetValue(globalObjectIdHash, out var prefab))
                {
                    switch (prefab.Override)
                    {
                        default:
                        case NetworkPrefabOverride.None:
                            networkPrefabReference = prefab.Prefab;
                            break;
                        case NetworkPrefabOverride.Hash:
                        case NetworkPrefabOverride.Prefab:
                            networkPrefabReference = prefab.OverridingTargetPrefab;
                            break;
                    }

                    // KEEPSAKE FIX - Addressable network prefabs
                    if (prefab.Override == NetworkPrefabOverride.None && prefab.Prefab == null && !string.IsNullOrEmpty(prefab.AddressableKey))
                    {
                        prefab.Prefab = await Addressables.LoadAssetAsync<GameObject>(prefab.AddressableKey).WithCancellation(NetworkManager.GetCancellationTokenOnDestroy());
                        networkPrefabReference = prefab.Prefab;
                    }
                    // END KEEPSAKE FIX
                }

                // If not, then there is an issue (user possibly didn't register the prefab properly?)
                if (networkPrefabReference == null)
                {
                    if (NetworkLog.CurrentLogLevel <= LogLevel.Error)
                    {
                        NetworkLog.LogError($"Failed to create object locally. [{nameof(globalObjectIdHash)}={globalObjectIdHash}]. {nameof(NetworkPrefab)} could not be found. Is the prefab registered with {nameof(NetworkManager)}?");
                    }

                    return null;
                }

                // Otherwise, instantiate an instance of the NetworkPrefab linked to the prefabHash
                // KEEPSAKE FIX - instantiate with parent provided so Awake can run at the proper place
                NetworkObject networkObject;
                if (parentNetworkObject != null)
                {
                    networkObject = Object.Instantiate(
                        networkPrefabReference,
                        position.GetValueOrDefault(Vector3.zero),
                        rotation.GetValueOrDefault(Quaternion.identity),
                        parentNetworkObject.transform).GetComponent<NetworkObject>();
                }
                else
                {
                    networkObject = (position == null && rotation == null
                        ? Object.Instantiate(networkPrefabReference)
                        : Object.Instantiate(
                            networkPrefabReference,
                            position.GetValueOrDefault(Vector3.zero),
                            rotation.GetValueOrDefault(Quaternion.identity))).GetComponent<NetworkObject>();
                }

                // KEEPSAKE FIX - helpful log message
                if (networkObject == null)
                {
                    if (NetworkLog.CurrentLogLevel <= LogLevel.Error)
                    {
                        NetworkLog.LogError($"Instantiated prefab {networkPrefabReference} does not contain a {nameof(NetworkObject)} component");
                    }
                    return null;
                }

                networkObject.NetworkManagerOwner = NetworkManager;

                // KEEPSAKE FIX - parent already set above
                /*if (parentNetworkObject != null)
                {
                    networkObject.transform.SetParent(parentNetworkObject.transform, true);
                }*/

                if (NetworkSceneManager.IsSpawnedObjectsPendingInDontDestroyOnLoad)
                {
                    Object.DontDestroyOnLoad(networkObject.gameObject);
                }

                return networkObject;
            }
        }
        {
            var networkObject = NetworkManager.SceneManager.GetSceneRelativeInSceneNetworkObject(globalObjectIdHash);

            if (networkObject == null)
            {
                if (NetworkLog.CurrentLogLevel <= LogLevel.Error)
                {
                    NetworkLog.LogError($"{nameof(NetworkPrefab)} hash was not found! In-Scene placed {nameof(NetworkObject)} soft synchronization failure for Hash: {globalObjectIdHash}!");
                }

                return null;
            }

            if (parentNetworkObject != null)
            {
                networkObject.transform.SetParent(parentNetworkObject.transform, true);
            }

            return networkObject;
        }
    }

    // Ran on both server and client
    internal void SpawnNetworkObjectLocally(NetworkObject networkObject, ulong networkId, bool sceneObject, bool playerObject, ulong? ownerClientId, bool destroyWithScene)
    {
        if (networkObject == null)
        {
            throw new ArgumentNullException(nameof(networkObject), "Cannot spawn null object");
        }

        // KEEPSAKE FIX
        if (networkObject.IsAttached)
        {
            throw new SpawnStateException("Object is already attached");
        }

        if (networkObject.IsSpawned)
        {
            throw new SpawnStateException("Object is already spawned");
        }

        SpawnNetworkObjectLocallyCommon(networkObject, networkId, sceneObject, playerObject, ownerClientId, destroyWithScene);
    }

    // Ran on both server and client
    internal void SpawnNetworkObjectLocally(NetworkObject networkObject, in NetworkObject.SceneObject sceneObject, FastBufferReader variableData, bool destroyWithScene)
    {
        if (networkObject == null)
        {
            throw new ArgumentNullException(nameof(networkObject), "Cannot spawn null object");
        }

        // KEEPSAKE FIX
        if (networkObject.IsAttached)
        {
            throw new SpawnStateException("Object is already attached");
        }

        if (networkObject.IsSpawned)
        {
            throw new SpawnStateException("Object is already spawned");
        }

        if (sceneObject.Header.HasNetworkVariables)
        {
            // KEEPSAKE FIX - we've added the variable data length to be able to skip over on fail, read it to advance the reader
            variableData.ReadValueSafe(out int _);

            networkObject.SetNetworkVariableData(variableData);

            // KEEPSAKE FIX - boundary marker
            #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
            if (!variableData.TryBeginRead(sizeof(int) + NetworkObject.SceneObject.MarkerObjectEndVars.Length))
            {
                throw new OverflowException($"Could not deserialize SceneObject {networkObject.NetworkObjectId} / {networkObject.GlobalObjectIdHash} end vars marker: Ran out of data (at byte {variableData.Position} of {variableData.Length}).");
            }
            variableData.ReadValue(out int markerLength);
            Assert.AreEqual(NetworkObject.SceneObject.MarkerObjectEndVars.Length, markerLength, $"NetworkObject {networkObject.NetworkObjectId} / {networkObject.GlobalObjectIdHash} end vars marker should be of expected length");
            for (var i = 0; i < markerLength; ++i)
            {
                variableData.ReadValue(out byte actualByte);
                Assert.AreEqual(NetworkObject.SceneObject.MarkerObjectEndVars[i], actualByte, $"NetworkObject {networkObject.NetworkObjectId} / {networkObject.GlobalObjectIdHash} end vars marker should be intact (byte {i})");
            }
            #endif
        }

        SpawnNetworkObjectLocallyCommon(networkObject, sceneObject.Header.NetworkObjectId, sceneObject.Header.IsSceneObject, sceneObject.Header.IsPlayerObject, sceneObject.Header.OwnerClientId, destroyWithScene);
    }

    // KEEPSAKE FIX
    internal void AttachNetworkObjectLocally(NetworkObject networkObject, ulong networkId, ulong? ownerClientId)
    {
        if (networkObject == null)
        {
            throw new ArgumentNullException(nameof(networkObject), "Cannot attach null object");
        }

        if (networkObject.IsAttached)
        {
            throw new SpawnStateException("Object is attached already");
        }

        AttachNetworkObjectLocallyCommon(networkObject, networkId, ownerClientId);
    }

    // KEEPSAKE FIX
    private void AttachNetworkObjectLocallyCommon(NetworkObject networkObject, ulong networkId, ulong? ownerClientId)
    {
        if (AttachedObjects.ContainsKey(networkId))
        {
            Debug.LogWarning($"Trying to attach {nameof(NetworkObject.NetworkObjectId)} {networkId} that is already attached!");
            return;
        }

        networkObject.IsAttached = true;
        networkObject.NetworkObjectId = networkId;
        networkObject.OwnerClientIdInternal = ownerClientId;

        //Debug.Log($"NetworkObject inst {networkObject.GetInstanceID()} ({networkObject.gameObject}) attached with ID {networkId} and owner {(ownerClientId.HasValue ? ownerClientId.Value : "None")}");

        AttachedObjects.Add(networkObject.NetworkObjectId, networkObject);
        AttachedObjectsList.Add(networkObject);

        networkObject.InvokeBehaviourOnNetworkObjectIdAssigned();

        if (ownerClientId != null && NetworkManager.IsServer)
        {
            NetworkManager.ConnectedClients[ownerClientId.Value].OwnedObjects.Add(networkObject);
        }

        if (NetworkManager.IsServer)
        {
            for (var i = 0; i < NetworkManager.ConnectedClientsList.Count; i++)
            {
                if (networkObject.CheckObjectVisibility == null || networkObject.CheckObjectVisibility(NetworkManager.ConnectedClientsList[i].ClientId))
                {
                    networkObject.Observers.Add(NetworkManager.ConnectedClientsList[i].ClientId);
                }
            }
        }

        // KEEPSAKE FIX - nesting, cleanup tracking
        if (networkObject.IsNested
            && NestedObjects.TryGetValue(networkObject.GlobalObjectIdHash, out var spawnedParentToInstanceMap)
            && spawnedParentToInstanceMap.TryGetValue(networkObject.SpawnedParentNetworkObjectId, out var nested)
            && nested == networkObject)
        {
            spawnedParentToInstanceMap.Remove(networkObject.SpawnedParentNetworkObjectId);
            if (spawnedParentToInstanceMap.Count == 0)
            {
                NestedObjects.Remove(networkObject.GlobalObjectIdHash);
            }
        }

        networkObject.SetCachedParent(networkObject.transform.parent);
        networkObject.ApplyNetworkParenting();
        NetworkObject.CheckOrphanChildren();

        networkObject.InvokeBehaviourNetworkAttach();

        NetworkManager.InterestManager.AddObject(ref networkObject);
    }

    // NOTE: called needs to return pathParts to array pool
    private static bool TryConstructRelativePathFromSpawnedParent(
        NetworkObject spawnedParent,
        NetworkObject networkObject,
        out string relativePath,
        out int depthToParent,
        out string[] pathParts,
        out int pathPartsLength)
    {
        var foundParent = false;
        depthToParent = 0;
        var p = networkObject.transform.parent;
        while (p != null)
        {
            if (p == spawnedParent.transform)
            {
                foundParent = true;
                break;
            }

            depthToParent++;
            p = p.parent;
        }

        if (!foundParent)
        {
            LogNetcode.Error(
                $"Unable to determine depth to parent from {networkObject.gameObject.Path()} to {spawnedParent.gameObject.Path()}");
            relativePath = default;
            pathParts = default;
            pathPartsLength = default;
            return false;
        }

        pathParts = ArrayPool<string>.Shared.Rent(depthToParent + 1); // +1 to include the networkObject in path
        pathPartsLength = 0; // assuming that path only consists ASCII, if path might be extra truncated so no big deal perhaps..
        var sb = new StringBuilder(pathPartsLength); // using a StringBuilder since string.Join over the pathParts with index and count takes the wrong overload...
        var parent = networkObject.transform.parent;
        for (var i = depthToParent - 1; i >= 0; --i)
        {
            // following the same path as while loop above so nothing should come up null
            pathParts[i] = parent.name;
            sb.Append(pathParts[i]).Append(k_PathPartsSeparator);
            pathPartsLength += pathParts[i].Length + 1; // +1 is pathPartsSeparator
            parent = parent.parent;
        }
        pathParts[depthToParent] = networkObject.gameObject.name;
        sb.Append(pathParts[depthToParent]);

        relativePath = sb.ToString();

        return true;
    }

    private void SpawnNetworkObjectLocallyCommon(NetworkObject networkObject, ulong networkId, bool sceneObject, bool playerObject, ulong? ownerClientId, bool destroyWithScene)
    {
        if (SpawnedObjects.ContainsKey(networkId))
        {
            Debug.LogWarning($"Trying to spawn {nameof(NetworkObject.NetworkObjectId)} {networkId} that already exists!");
            return;
        }

        // this initialization really should be at the bottom of the function
        networkObject.IsSpawned = true;

        // this initialization really should be at the top of this function.  If and when we break the
        //  NetworkVariable dependency on NetworkBehaviour, this otherwise creates problems because
        //  SetNetworkVariableData above calls InitializeVariables, and the 'baked out' data isn't ready there;
        //  the current design banks on getting the network behaviour set and then only reading from it
        //  after the below initialization code.  However cowardice compels me to hold off on moving this until
        //  that commit
        networkObject.IsSceneObject = sceneObject;
        // KEEPSAKE FIX - done in attach
        //networkObject.NetworkObjectId = networkId;

        networkObject.DestroyWithScene = sceneObject || destroyWithScene;

        // KEEPSAKE FIX - done in attach
        //networkObject.OwnerClientIdInternal = ownerClientId;
        networkObject.IsPlayerObject = playerObject;

        // KEEPSAKE FIX
        AttachNetworkObjectLocallyCommon(networkObject, networkId, ownerClientId);

        SpawnedObjects.Add(networkObject.NetworkObjectId, networkObject);
        SpawnedObjectsList.Add(networkObject);

        if (ownerClientId != null)
        {
            if (NetworkManager.IsServer)
            {
                if (playerObject)
                {
                    NetworkManager.ConnectedClients[ownerClientId.Value].PlayerObject = networkObject;
                }
                // KEEPSAKE FIX - done in attach
                /*
                else
                {
                    NetworkManager.ConnectedClients[ownerClientId.Value].OwnedObjects.Add(networkObject);
                }*/
            }
            else if (playerObject && ownerClientId.Value == NetworkManager.LocalClientId)
            {
                NetworkManager.LocalClient.PlayerObject = networkObject;
            }
        }

        // KEEPSAKE FIX - done in attach
        /*if (NetworkManager.IsServer)
        {
            for (int i = 0; i < NetworkManager.ConnectedClientsList.Count; i++)
            {
                if (networkObject.CheckObjectVisibility == null || networkObject.CheckObjectVisibility(NetworkManager.ConnectedClientsList[i].ClientId))
                {
                    networkObject.Observers.Add(NetworkManager.ConnectedClientsList[i].ClientId);
                }
            }
        }

        networkObject.SetCachedParent(networkObject.transform.parent);
        networkObject.ApplyNetworkParenting();
        NetworkObject.CheckOrphanChildren();*/

        // KEEPSAKE FIX - attach any nested NetworkObjects, clients still find nested and map their IDs to be able to attach later
        if (!sceneObject)
        {
            foreach (var nested in networkObject.gameObject.GetComponentsInChildren<NetworkObject>(true))
            {
                if (nested.IsNested)
                {
                    nested.IsSceneObject = false;
                    nested.DestroyWithScene = networkObject.DestroyWithScene;
                    nested.SpawnedParentNetworkObjectId = networkObject.NetworkObjectId;
                    nested.OwnerClientIdInternal = ownerClientId;

                    if (!NestedObjects.TryGetValue(nested.GlobalObjectIdHash, out var spawnedParentToNestedInstanceMap))
                    {
                        spawnedParentToNestedInstanceMap = new Dictionary<ulong, NetworkObject>();
                        NestedObjects[nested.GlobalObjectIdHash] = spawnedParentToNestedInstanceMap;
                    }

                    if (spawnedParentToNestedInstanceMap.TryGetValue(networkObject.NetworkObjectId, out var existingNestedInstance))
                    {
                        LogNetcode.Error($"Spawned parent {networkObject.NetworkObjectId} already contains a nested instance with hash {nested.GlobalObjectIdHash}. Something is wrong, all nested instances should have been generated unique global ids. New: {nested.gameObject.Path(true)} -- Existing: {existingNestedInstance.gameObject.Path(true)}");
                    }
                    else
                    {
                        spawnedParentToNestedInstanceMap[networkObject.NetworkObjectId] = nested;
                    }

                    if (NetworkManager.IsServer)
                    {
                        nested.Attach();
                    }
                }
            }
        }

        //Debug.Log($"NetworkObject ({networkObject.gameObject}) with ID {networkId} spawned");

        networkObject.InvokeBehaviourNetworkSpawn();

        // KEEPSAKE FIX - done in attach
        //NetworkManager.InterestManager.AddObject(ref networkObject);

        // This must happen after InvokeBehaviourNetworkSpawn, otherwise ClientRPCs and other messages can be
        // processed before the object is fully spawned. This must be the last thing done in the spawn process.
        if (m_Triggers.ContainsKey(networkId))
        {
            var triggerInfo = m_Triggers[networkId];
            foreach (var trigger in triggerInfo.TriggerData)
            {
                // Reader will be disposed within HandleMessage
                NetworkManager.MessagingSystem.HandleMessage(trigger.Header, trigger.Reader, trigger.SenderId, trigger.Timestamp, trigger.SerializedHeaderSize);
            }

            triggerInfo.TriggerData.Dispose();
            m_Triggers.Remove(networkId);
        }
    }

    internal ulong? GetSpawnParentId(NetworkObject networkObject)
    {
        NetworkObject parentNetworkObject = null;

        if (!networkObject.AlwaysReplicateAsRoot && networkObject.transform.parent != null)
        {
            parentNetworkObject = networkObject.transform.parent.GetComponent<NetworkObject>();

            // KEEPSAKE FIX - treat this as an error since we really want to retain hierarchy between peers
            if (parentNetworkObject == null)
            {
                Debug.LogError($"Parent '{networkObject.transform.parent.gameObject.name}' of spawned object '{networkObject.gameObject.name}' is invalid. It needs to have the NetworkObject component to be a viable parent. '{networkObject.gameObject}' will spawn an orphan on clients!");
            }
        }

        if (parentNetworkObject == null)
        {
            return null;
        }

        return parentNetworkObject.NetworkObjectId;
    }

    internal void DespawnObject(NetworkObject networkObject, bool destroyObject = false)
    {
        if (!networkObject.IsSpawned)
        {
            throw new SpawnStateException("Object is not spawned");
        }

        if (!NetworkManager.IsServer)
        {
            throw new NotServerException("Only server can despawn objects");
        }

        OnDespawnObject(networkObject, destroyObject);
    }

    // Makes scene objects ready to be reused
    internal void ServerResetShudownStateForSceneObjects()
    {
        foreach (var sobj in SpawnedObjectsList)
        {
            if ((sobj.IsSceneObject != null && sobj.IsSceneObject == true) || sobj.DestroyWithScene)
            {
                sobj.IsSpawned = false;
                sobj.IsAttached = false; // KEEPSAKE FIX
                sobj.DestroyWithScene = false;
                sobj.IsSceneObject = null;
            }
        }
    }

    /// <summary>
    ///     Gets called only by NetworkSceneManager.SwitchScene
    /// </summary>
    internal void ServerDestroySpawnedSceneObjects()
    {
        // This Allocation is "OK" for now because this code only executes when a new scene is switched to
        // We need to create a new copy the HashSet of NetworkObjects (SpawnedObjectsList) so we can remove
        // objects from the HashSet (SpawnedObjectsList) without causing a list has been modified exception to occur.
        var spawnedObjects = SpawnedObjectsList.ToList();

        foreach (var sobj in spawnedObjects)
        {
            if (sobj.IsSceneObject != null && sobj.IsSceneObject.Value && sobj.DestroyWithScene && sobj.gameObject.scene != NetworkManager.SceneManager.DontDestroyOnLoadScene)
            {
                //Debug.Log($"NetworkObject inst {sobj.GetInstanceID()} ({sobj.gameObject}) with ID {sobj.NetworkObjectId} removed as part of {nameof(ServerDestroySpawnedSceneObjects)}");

                SpawnedObjectsList.Remove(sobj);
                AttachedObjectsList.Remove(sobj); // KEEPSAKE FIX
                Object.Destroy(sobj.gameObject);
            }
        }
    }

    internal void DespawnAndDestroyNetworkObjects()
    {
        var networkObjects = Object.FindObjectsOfType<NetworkObject>();

        for (var i = 0; i < networkObjects.Length; i++)
        {
            if (networkObjects[i].NetworkManager == NetworkManager)
            {
                if (NetworkManager.PrefabHandler.ContainsHandler(networkObjects[i]))
                {
                    OnDespawnObject(networkObjects[i], false);
                    // Leave destruction up to the handler
                    NetworkManager.PrefabHandler.HandleNetworkPrefabDestroy(networkObjects[i]);
                }
                else if (networkObjects[i].IsSpawned)
                {
                    // If it is an in-scene placed NetworkObject then just despawn
                    // and let it be destroyed when the scene is unloaded.  Otherwise,
                    // despawn and destroy it.
                    var shouldDestroy = !(networkObjects[i].IsSceneObject != null && networkObjects[i].IsSceneObject.Value);

                    OnDespawnObject(networkObjects[i], shouldDestroy);
                }
                else
                {
                    Object.Destroy(networkObjects[i].gameObject);
                }
            }
        }
    }

    // KEEPSAKE FIX - make public
    public void DestroySceneObjects()
    {
        var networkObjects = Object.FindObjectsOfType<NetworkObject>();

        for (var i = 0; i < networkObjects.Length; i++)
        {
            if (networkObjects[i].NetworkManager == NetworkManager)
            {
                if (networkObjects[i].IsSceneObject == null || networkObjects[i].IsSceneObject.Value)
                {
                    if (NetworkManager.PrefabHandler.ContainsHandler(networkObjects[i]))
                    {
                        NetworkManager.PrefabHandler.HandleNetworkPrefabDestroy(networkObjects[i]);
                        // KEEPSAKE FIX - find objects in Attached instead of Spawned collection
                        if (AttachedObjects.ContainsKey(networkObjects[i].NetworkObjectId))
                        {
                            OnDespawnObject(networkObjects[i], false);
                        }
                    }
                    else
                    {
                        Object.Destroy(networkObjects[i].gameObject);
                    }
                }
            }
        }
    }

    internal void ServerSpawnSceneObjectsOnStartSweep()
    {
        var networkObjects = Object.FindObjectsOfType<NetworkObject>();

        for (var i = 0; i < networkObjects.Length; i++)
        {
            if (networkObjects[i].NetworkManager == NetworkManager)
            {
                if (networkObjects[i].IsSceneObject == null)
                {
                    SpawnNetworkObjectLocally(networkObjects[i], GetNetworkObjectId(), true, false, null, true);
                }
            }
        }
    }

    internal void OnDespawnObject(NetworkObject networkObject, bool destroyGameObject)
    {
        if (NetworkManager == null)
        {
            return;
        }

        // We have to do this check first as subsequent checks assume we can access NetworkObjectId.
        if (networkObject == null)
        {
            Debug.LogWarning("Trying to destroy network object but it is null");
            return;
        }

        // Removal of spawned object
        // KEEPSAKE FIX - find objects in Attached instead of Spawned collection
        if (!AttachedObjects.ContainsKey(networkObject.NetworkObjectId))
        {
            Debug.LogWarning($"Trying to destroy object {networkObject.NetworkObjectId} but it doesn't seem to exist anymore!");
            return;
        }

        // If we are shutting down the NetworkManager, then ignore resetting the parent
        if (!NetworkManager.ShutdownInProgress)
        {
            // Move child NetworkObjects to the root when parent NetworkObject is destroyed
            // KEEPSAKE FIX - find objects in Attached instead of Spawned collection
            foreach (var spawnedNetObj in AttachedObjectsList)
            {
                var (isReparented, latestParent) = spawnedNetObj.GetNetworkParenting();
                if (isReparented && latestParent == networkObject.NetworkObjectId)
                {
                    spawnedNetObj.gameObject.transform.parent = null;

                    if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
                    {
                        NetworkLog.LogWarning($"{nameof(NetworkObject)} #{spawnedNetObj.NetworkObjectId} moved to the root because its parent {nameof(NetworkObject)} #{networkObject.NetworkObjectId} is destroyed");
                    }
                }
            }
        }

        if (!networkObject.IsOwnedByServer && !networkObject.IsPlayerObject && TryGetNetworkClient(networkObject.OwnerClientId, out var networkClient))
        {
            //Someone owns it.
            for (var i = networkClient.OwnedObjects.Count - 1; i > -1; i--)
            {
                if (networkClient.OwnedObjects[i].NetworkObjectId == networkObject.NetworkObjectId)
                {
                    networkClient.OwnedObjects.RemoveAt(i);
                }
            }
        }

        networkObject.InvokeBehaviourNetworkDespawn();

        // KEEPSAKE FIX
        m_ObjectDespawnedSubject.OnNext(networkObject);

        if (NetworkManager != null && NetworkManager.IsServer)
        {
            if (NetworkManager.NetworkConfig.RecycleNetworkIds)
            {
                ReleasedNetworkObjectIds.Enqueue(
                    new ReleasedNetworkId
                    {
                        NetworkId = networkObject.NetworkObjectId,
                        ReleaseTime = Time.unscaledTime,
                    });
            }

            // KEEPSAKE FIX - don't send Despawn commands for Attached-spawned-elsewhere objects
            if (networkObject.IsSpawned)
            {
                networkObject.SnapshotDespawn();
            }
        }

        networkObject.IsSpawned = false;
        if (SpawnedObjects.Remove(networkObject.NetworkObjectId))
        {
            SpawnedObjectsList.Remove(networkObject);
        }

        // KEEPSAKE FIX
        //Debug.Log($"NetworkObject inst {networkObject.GetInstanceID()} ({networkObject.gameObject}) with ID {networkObject.NetworkObjectId} removed as part of {nameof(DespawnObject)}");
        networkObject.IsAttached = false;
        if (AttachedObjects.Remove(networkObject.NetworkObjectId))
        {
            AttachedObjectsList.Remove(networkObject);
        }

        // KEEPSAKE FIX - cleanup knowledge of nested
        if (networkObject.IsNested)
        {
            if (NestedObjects.TryGetValue(networkObject.GlobalObjectIdHash, out var spawnedParentToInstanceMap)
                && spawnedParentToInstanceMap.TryGetValue(networkObject.SpawnedParentNetworkObjectId, out var nested)
                && nested == networkObject)
            {
                spawnedParentToInstanceMap.Remove(networkObject.SpawnedParentNetworkObjectId);
                if (spawnedParentToInstanceMap.Count == 0)
                {
                    NestedObjects.Remove(networkObject.GlobalObjectIdHash);
                }
            }
        }
        else
        {
            // Remove us as "spawned parent"
            // This shouldn't be necessary given that all nested objects clean themselves up per the code above

            foreach (var (_, spawnedParentToInstanceMap) in NestedObjects)
            {
                spawnedParentToInstanceMap.Remove(networkObject.NetworkObjectId);
            }
        }

        NetworkManager.InterestManager.RemoveObject(ref networkObject);

        var gobj = networkObject.gameObject;
        if (destroyGameObject && gobj != null)
        {
            if (NetworkManager.PrefabHandler.ContainsHandler(networkObject))
            {
                NetworkManager.PrefabHandler.HandleNetworkPrefabDestroy(networkObject);
            }
            else
            {
                Object.Destroy(gobj);
            }
        }
    }

    /// <summary>
    ///     Updates all spawned <see cref="NetworkObject.Observers" /> for the specified client
    ///     Note: if the clientId is the server then it is observable to all spawned <see cref="NetworkObject" />'s
    /// </summary>
    internal void UpdateObservedNetworkObjects(ulong clientId)
    {
        // KEEPSAKE FIX - find objects in Attached instead of Spawned collection
        foreach (var sobj in AttachedObjectsList)
        {
            if (sobj.CheckObjectVisibility == null || NetworkManager.IsServer)
            {
                if (!sobj.Observers.Contains(clientId))
                {
                    sobj.Observers.Add(clientId);
                }
            }
            else
            {
                if (sobj.CheckObjectVisibility(clientId))
                {
                    sobj.Observers.Add(clientId);
                }
                else if (sobj.Observers.Contains(clientId))
                {
                    sobj.Observers.Remove(clientId);
                }
            }
        }
    }

    private struct TriggerData
    {
        public FastBufferReader Reader;
        public MessageHeader    Header;
        public ulong            SenderId;
        public float            Timestamp;
        public int              SerializedHeaderSize;
    }

    private struct TriggerInfo
    {
        public float                   Expiry;
        public NativeList<TriggerData> TriggerData;
    }
}

}
