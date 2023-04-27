using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Cysharp.Threading.Tasks;
using Keepsake.Common;
using Keepsake.Common.Extensions;
using UnityEditor;
using UnityEngine;
using UnityEngine.Assertions;
using UnityEngine.Pool;
using UnityEngine.SceneManagement;
#if UNITY_EDITOR
using Unity.Netcode.SceneManagement;
using UnityEditor.AddressableAssets;
#endif

namespace Unity.Netcode
{

/// <summary>
///     A component used to identify that a GameObject in the network
/// </summary>
// KEEPSAKE FIX - Hide in menu, we use our own wrapper with RequireComponent
//[AddComponentMenu("Netcode/" + nameof(NetworkObject), -99)]
[AddComponentMenu("")]
[DisallowMultipleComponent]
public sealed class NetworkObject : MonoBehaviour
{
    /// <summary>
    ///     Delegate type for checking spawn options
    /// </summary>
    /// <param name="clientId">The clientId to check spawn options for</param>
    public delegate bool SpawnDelegate(ulong clientId);

    /// <summary>
    ///     Delegate type for checking visibility
    /// </summary>
    /// <param name="clientId">The clientId to check visibility for</param>
    public delegate bool VisibilityDelegate(ulong clientId);

    // We're keeping this set called OrphanChildren which contains NetworkObjects
    // because at the time we initialize/spawn NetworkObject locally, we might not have its parent replicated from the other side
    //
    // For instance, if we're spawning NetworkObject 5 and its parent is 10, what should happen if we do not have 10 yet?
    // let's say 10 is on the way to be replicated in a few frames and we could fix that parent-child relationship later.
    //
    // If you couldn't find your parent, we put you into OrphanChildren set and everytime we spawn another NetworkObject locally due to replication,
    // we call CheckOrphanChildren() method and quickly iterate over OrphanChildren set and see if we can reparent/adopt one.
    internal static HashSet<NetworkObject> OrphanChildren = new();

    /// <summary>
    ///     If true, the object will always be replicated as root on clients and the parent will be ignored.
    /// </summary>
    public bool AlwaysReplicateAsRoot;

    /// <summary>
    ///     Whether or not to enable automatic NetworkObject parent synchronization.
    /// </summary>
    public bool AutoObjectParentSync = false; // KEEPSAKE FIX - default to false since we already control parenting of most things

    /// <summary>
    ///     Delegate invoked when the netcode needs to know if the object should be visible to a client, if null it will assume
    ///     true
    /// </summary>
    public VisibilityDelegate CheckObjectVisibility = null;

    // KEEPSAKE FIX
    public Action<NetworkObject> MovedToNewScene;

    /// <summary>
    ///     Whether or not to destroy this object if it's owner is destroyed.
    ///     If false, the objects ownership will be given to the server.
    /// </summary>
    /// KEEPSAKE FIX - removed negation in boolean name (very confusing) and default to not destroying with owner
    //public bool DontDestroyWithOwner;
    public bool DestroyWithOwner;

    /// <summary>
    ///     Delegate invoked when the netcode needs to know if it should include the transform when spawning the object, if
    ///     null it will assume true
    /// </summary>
    public SpawnDelegate IncludeTransformWhenSpawning = null;

    // KEEPSAKE FIX
    /// <summary>
    ///     The lowest snapshot group this Network Object should first appear in.
    ///     Clients will receive group 0 when they initially connect, and must opt-in to receive other groups later on.
    ///     This is so that special data can trickle to clients during their loading screen / connection phase but the large
    ///     bulk of world state snapshots
    ///     will wait until they are fully connected and have joined the game.
    /// </summary>
    [Range(byte.MinValue, byte.MaxValue)]
    public byte m_SnapshotGroup = 100;

    // KEEPSAKE FIX - make public
    public readonly HashSet<ulong> Observers = new();

    [HideInInspector]
    [SerializeField]
    public uint GlobalObjectIdHash;

    [HideInInspector]
    [SerializeField]
    // KEEPSAKE FIX - to debug what input the hash is based on, could be stripped in builds or whatever
    public string GlobalObjectIdHash_DebugInput;

    public delegate uint HashingAlgorithmDelegate(string input);

    public static HashingAlgorithmDelegate HashingAlgorithm => XXHash.Hash32;

    /// <summary>
    ///     The NetworkManager that owns this NetworkObject.
    ///     This property controls where this NetworkObject belongs.
    ///     This property is null by default currently, which means that the above NetworkManager getter will return the
    ///     Singleton.
    ///     In the future this is the path where alternative NetworkManagers should be injected for running multi
    ///     NetworkManagers
    /// </summary>
    internal NetworkManager NetworkManagerOwner;

    internal ulong? OwnerClientIdInternal = null;

    #if MULTIPLAYER_TOOLS
    private string m_CachedNameForMetrics;
    #endif
    private Transform m_CachedParent; // What is our last set parent Transform reference?

    // KEEPSAKE FIX - also include INetworkRpcHandlers
    private List<(NetworkBehaviour, INetworkRpcHandler)> m_ChildNetworkBehaviours;
    private List<NetworkBehaviour> m_ChildNetworkBehaviours_BehavioursOnly;

    private bool   m_IsReparented; // Did initial parent (came from the scene hierarchy) change at runtime?
    private ulong? m_LatestParent; // What is our last set parent NetworkObject's ID?

    /// <summary>
    ///     Gets the NetworkManager that owns this NetworkObject instance
    /// </summary>
    public NetworkManager NetworkManager => NetworkManagerOwner ?? NetworkManager.Singleton;

    // KEEPSAKE FIX - commented out, property below is used instead, wanted to clarify /TM
    //private ulong m_NetworkObjectId;

    /// <summary>
    ///     Gets the unique Id of this object that is synced across the network
    /// </summary>
    public ulong NetworkObjectId { get; internal set; }
    // END KEEPSAKE FIX

    // KEEPSAKE FIX - internal check for destroy to prevent multiple calls to network despawn/parent destroy events
    public bool IsBeingDestroyed { get; internal set; }
    // END KEEPSAKE FIX

    // KEEPSAKE FIX - some state to support spawning nested NetworkObjects
    internal ulong RootNetworkObjectId { get; set; }

    /// <summary>
    ///     Gets the ClientId of the owner of this NetworkObject
    /// </summary>
    public ulong OwnerClientId
    {
        get
        {
            if (OwnerClientIdInternal == null)
            {
                return NetworkManager != null ? NetworkManager.ServerClientId : 0;
            }
            else
            {
                return OwnerClientIdInternal.Value;
            }
        }
        internal set
        {
            if (NetworkManager != null && value == NetworkManager.ServerClientId)
            {
                OwnerClientIdInternal = null;
            }
            else
            {
                OwnerClientIdInternal = value;
            }
        }
    }

    /// <summary>
    ///     Gets if this object is a player object
    /// </summary>
    public bool IsPlayerObject { get; internal set; }

    /// <summary>
    ///     Gets if the object is the the personal clients player object
    /// </summary>
    public bool IsLocalPlayer => NetworkManager != null && IsPlayerObject && OwnerClientId == NetworkManager.LocalClientId;

    /// <summary>
    ///     Gets if the object is owned by the local player or if the object is the local player object
    /// </summary>
    public bool IsOwner => NetworkManager != null && OwnerClientId == NetworkManager.LocalClientId;

    /// <summary>
    ///     Gets Whether or not the object is owned by anyone
    /// </summary>
    public bool IsOwnedByServer => NetworkManager != null && OwnerClientId == NetworkManager.ServerClientId;

    /// <summary>
    ///     Gets if the object has yet been spawned across the network
    /// </summary>
    public bool IsSpawned { get; internal set; }

    // KEEPSAKE FIX concept of Attach
    public bool IsAttached { get; internal set; }

    /// <summary>
    ///     Gets if the object is a SceneObject, null if it's not yet spawned but is a scene object.
    /// </summary>
    public bool? IsSceneObject { get; internal set; }

    /// <summary>
    /// KEEPSAKE FIX - Gets if the object is instantiated as being nested inside another NetworkObject
    /// </summary>
    public bool IsNested { get; internal set; }

    /// <summary>
    ///     Gets whether or not the object should be automatically removed when the scene is unloaded.
    /// </summary>
    public bool DestroyWithScene { get; set; }

    // KEEPSAKE FIX - made public
    public List<(NetworkBehaviour, INetworkRpcHandler)> ChildNetworkBehaviours
    {
        get
        {
            if (m_ChildNetworkBehaviours != null)
            {
                return m_ChildNetworkBehaviours;
            }

            m_ChildNetworkBehaviours = new List<(NetworkBehaviour, INetworkRpcHandler)>();
            var networkBehaviours = GetComponentsInChildren<NetworkBehaviour>(true);
            for (var i = 0; i < networkBehaviours.Length; i++)
            {
                if (networkBehaviours[i].NetworkObject == this)
                {
                    m_ChildNetworkBehaviours.Add((networkBehaviours[i], null));
                }
            }

            // KEEPSAKE FIX - include INetworkRpcHandlers in the list
            var networkRpcHandlers = GetComponentsInChildren<INetworkRpcHandler>(true);
            for (var i = 0; i < networkRpcHandlers.Length; i++)
            {
                if (networkRpcHandlers[i].NetworkObject == this)
                {
                    m_ChildNetworkBehaviours.Add((null, networkRpcHandlers[i]));
                }
            }

            return m_ChildNetworkBehaviours;
        }
    }

    // KEEPSAKE FIX - since we added INetworkRpcHandler to ChildNetworkBehaviours we also add this "view" of the same data with only true NetworkBehaviours
    // ReSharper disable once InconsistentNaming
    public List<NetworkBehaviour> ChildNetworkBehaviours_BehavioursOnly
    {
        get
        {
            if (m_ChildNetworkBehaviours_BehavioursOnly != null)
            {
                return m_ChildNetworkBehaviours_BehavioursOnly;
            }

            var all = ChildNetworkBehaviours;
            m_ChildNetworkBehaviours_BehavioursOnly = new List<NetworkBehaviour>(all.Count);
            foreach (var (behaviour, _) in all)
            {
                if (behaviour == null)
                {
                    continue;
                }

                m_ChildNetworkBehaviours_BehavioursOnly.Add(behaviour);
            }

            return m_ChildNetworkBehaviours_BehavioursOnly;
        }
    }

    // KEEPSAKE FIX
    public const string AddressableLabel = "__Internal_NetworkSpawnable";

    internal string GetNameForMetrics()
    {
        #if MULTIPLAYER_TOOLS
        return m_CachedNameForMetrics ??= name;
        #else
            return null;
        #endif
    }

    /// <summary>
    ///     Returns Observers enumerator
    /// </summary>
    /// <returns>Observers enumerator</returns>
    public HashSet<ulong>.Enumerator GetObservers()
    {
        // KEEPSAKE FIX - check IsAttached and not IsSpawned
        if (!IsAttached)
        {
            throw new SpawnStateException($"Cannot {nameof(GetObservers)} on NetworkObject {gameObject.Path(true)} because it is not spawned with the network layer. If {gameObject.name} was just instantiated, it is probably too early in its lifetime. Try doing this later, from e.g. NetworkSetup or SafeStart.");
        }

        return Observers.GetEnumerator();
    }

    /// <summary>
    ///     Whether or not this object is visible to a specific client
    /// </summary>
    /// <param name="clientId">The clientId of the client</param>
    /// <returns>True if the client knows about the object</returns>
    public bool IsNetworkVisibleTo(ulong clientId)
    {
        // KEEPSAKE FIX - check IsAttached and not IsSpawned
        if (!IsAttached)
        {
            throw new SpawnStateException($"Cannot check {nameof(IsNetworkVisibleTo)} on NetworkObject {gameObject.Path(true)} because it is not spawned with the network layer. If {gameObject.name} was just instantiated, it is probably too early in its lifetime. Try doing this later, from e.g. NetworkSetup or SafeStart.");
        }

        return Observers.Contains(clientId);
    }

    private void Awake()
    {
        SetCachedParent(transform.parent);

        // KEEPSAKE FIX - taint all nested NetworkObjects so they know they belong to someone else (we might also be nested so it might not be us)
        foreach (var nested in GetComponentsInChildren<NetworkObject>(true))
        {
            if (nested != this)
            {
                nested.IsNested = true;
            }
        }
    }

    /// <summary>
    ///     Shows a previously hidden <see cref="NetworkObject" /> to a client
    /// </summary>
    /// <param name="clientId">The client to show the <see cref="NetworkObject" /> to</param>
    public void NetworkShow(ulong clientId)
    {
        if (!IsSpawned)
        {
            throw new SpawnStateException("Object is not spawned");
        }

        if (!NetworkManager.IsServer)
        {
            throw new NotServerException("Only server can change visibility");
        }

        if (Observers.Contains(clientId))
        {
            throw new VisibilityChangeException("The object is already visible");
        }

        // KEEPSAKE FIX - nesting, ensure parents are spawned on client first, otherwise an invalid spawn command (referring to a parent that is null) will be sent
        foreach (var no in GetComponentsInParent<NetworkObject>(true))
        {
            // also comparing GameObjects to not cause infinite loop when encountering malformed networked prefabs with 2 NetworkObject components.. :scream:
            if (no == this || no.gameObject == gameObject)
            {
                continue;
            }

            if (!no.Observers.Contains(clientId))
            {
                no.NetworkShow(clientId);
            }

            // other parents visited recursively above
            break;
        }
        // END KEEPSAKE FIX

        SnapshotSpawn(clientId);

        Observers.Add(clientId);
    }

    /// <summary>
    ///     Shows a list of previously hidden <see cref="NetworkObject" />s to a client
    /// </summary>
    /// <param name="networkObjects">The <see cref="NetworkObject" />s to show</param>
    /// <param name="clientId">The client to show the objects to</param>
    public static void NetworkShow(List<NetworkObject> networkObjects, ulong clientId)
    {
        if (networkObjects == null || networkObjects.Count == 0)
        {
            throw new ArgumentNullException("At least one " + nameof(NetworkObject) + " has to be provided");
        }

        var networkManager = networkObjects[0].NetworkManager;

        if (!networkManager.IsServer)
        {
            throw new NotServerException("Only server can change visibility");
        }

        // Do the safety loop first to prevent putting the netcode in an invalid state.
        for (var i = 0; i < networkObjects.Count; i++)
        {
            // KEEPSAKE FIX - check IsAttached and not IsSpawned
            if (!networkObjects[i].IsAttached)
            {
                throw new SpawnStateException($"Cannot {nameof(NetworkShow)} NetworkObject {networkObjects[i].gameObject.Path(true)} because it is not spawned with the network layer. If {networkObjects[i].gameObject.name} was just instantiated, it is probably too early in its lifetime. Try doing this later, from e.g. NetworkSetup or SafeStart.");
            }

            if (networkObjects[i].Observers.Contains(clientId))
            {
                throw new VisibilityChangeException($"{nameof(NetworkObject)} with NetworkId: {networkObjects[i].NetworkObjectId} is already visible");
            }

            if (networkObjects[i].NetworkManager != networkManager)
            {
                throw new ArgumentNullException("All " + nameof(NetworkObject) + "s must belong to the same " + nameof(NetworkManager));
            }
        }

        foreach (var networkObject in networkObjects)
        {
            networkObject.NetworkShow(clientId);
        }
    }

    /// <summary>
    ///     Hides a object from a specific client
    /// </summary>
    /// <param name="clientId">The client to hide the object for</param>
    public void NetworkHide(ulong clientId)
    {
        // KEEPSAKE NOTE: hiding and showing nested sub-objects not (yet?) supported, only truly spawned objects..
        if (!IsSpawned)
        {
            throw new SpawnStateException("Object is not spawned");
        }

        if (!NetworkManager.IsServer)
        {
            throw new NotServerException("Only server can change visibility");
        }

        if (!Observers.Contains(clientId))
        {
            throw new VisibilityChangeException("The object is already hidden");
        }

        if (clientId == NetworkManager.ServerClientId)
        {
            throw new VisibilityChangeException("Cannot hide an object from the server");
        }

        Observers.Remove(clientId);

        SnapshotDespawn(clientId);
    }

    /// <summary>
    ///     Hides a list of objects from a client
    /// </summary>
    /// <param name="networkObjects">The objects to hide</param>
    /// <param name="clientId">The client to hide the objects from</param>
    public static void NetworkHide(List<NetworkObject> networkObjects, ulong clientId)
    {
        if (networkObjects == null || networkObjects.Count == 0)
        {
            throw new ArgumentNullException("At least one " + nameof(NetworkObject) + " has to be provided");
        }

        var networkManager = networkObjects[0].NetworkManager;

        if (!networkManager.IsServer)
        {
            throw new NotServerException("Only server can change visibility");
        }

        if (clientId == networkManager.ServerClientId)
        {
            throw new VisibilityChangeException("Cannot hide an object from the server");
        }

        // Do the safety loop first to prevent putting the netcode in an invalid state.
        for (var i = 0; i < networkObjects.Count; i++)
        {
            // KEEPSAKE FIX - check IsAttached and not IsSpawned
            if (!networkObjects[i].IsAttached)
            {
                throw new SpawnStateException("Object is not attached");
            }

            if (!networkObjects[i].Observers.Contains(clientId))
            {
                throw new VisibilityChangeException($"{nameof(NetworkObject)} with {nameof(NetworkObjectId)}: {networkObjects[i].NetworkObjectId} is already hidden");
            }

            if (networkObjects[i].NetworkManager != networkManager)
            {
                throw new ArgumentNullException("All " + nameof(NetworkObject) + "s must belong to the same " + nameof(NetworkManager));
            }
        }

        foreach (var networkObject in networkObjects)
        {
            networkObject.NetworkHide(clientId);
        }
    }

    /// <summary>
    /// KEEPSAKE FIX
    /// This needs to be called when a NetworkObject is moved to a different scene, so that e.g. the visibility check can be recomputed.
    /// </summary>
    public void NotifyMovedToNewScene()
    {
        MovedToNewScene?.Invoke(this);
    }

    private void OnDestroy()
    {
        if (NetworkManager != null && NetworkManager.IsListening && NetworkManager.IsServer == false && IsSpawned && (IsSceneObject == null || (IsSceneObject != null && IsSceneObject.Value != true)))
        {
            throw new NotServerException($"Destroy a spawned {nameof(NetworkObject)} ({gameObject}) on a non-host client is not valid. Call {nameof(Destroy)} or {nameof(Despawn)} on the server/host instead.");
        }

        // KEEPSAKE FIX - Set to true because we are entering OnDespawn from OnDestroy
        IsBeingDestroyed = true;
        // END KEEPSAKE FIX

        if (NetworkManager != null && NetworkManager.SpawnManager != null)
        {
            // KEEPSAKE FIX check AttachedObjects instead of SpawnedObjects
            if (NetworkManager.SpawnManager.AttachedObjects.TryGetValue(NetworkObjectId, out var networkObject))
            {
                NetworkManager.SpawnManager.OnDespawnObject(networkObject, false);
            }
        }
    }

    private SnapshotDespawnCommand GetDespawnCommand()
    {
        var command = new SnapshotDespawnCommand();
        command.NetworkObjectId = NetworkObjectId;
        // KEEPSAKE FIX - Pass along IsBeingDestroyed
        command.IsBeingDestroyed = IsBeingDestroyed;
        // END KEEPSAKE FIX

        return command;
    }

    private SnapshotSpawnCommand GetSpawnCommand()
    {
        var command = new SnapshotSpawnCommand();
        command.NetworkObjectId = NetworkObjectId;
        command.RootNetworkObjectId = RootNetworkObjectId;
        command.OwnerClientId = OwnerClientId;
        command.IsPlayerObject = IsPlayerObject;
        command.IsSceneObject = IsSceneObject == null || IsSceneObject.Value;

        // KEEPSAKE FIX - renamed to clarify variable
        var hasNetworkedParent = NetworkSpawnManager.TryGetSpawnParentId(this, out var parentNetworkObject);
        if (hasNetworkedParent)
        {
            command.ParentNetworkId = parentNetworkObject.NetworkObjectId;

            // KEEPSAKE FIX
            command.WaitForParentIfMissing = parentNetworkObject.IsAttached && !parentNetworkObject.IsSpawned;

            // KEEPSAKE FIX - encode position and rotation in parent space when spawning with parent so that client spawns in correct spot in relation to parent
            var parentTransform = parentNetworkObject.transform;
            command.ObjectPositionLocal = parentTransform.InverseTransformPoint(transform.position);
            command.ObjectRotationLocal = Quaternion.Inverse(parentTransform.rotation) * transform.rotation;
        }
        else
        {
            // write own network id, when no parents. todo: optimize this.
            command.ParentNetworkId = command.NetworkObjectId;
            command.WaitForParentIfMissing = false; // KEEPSAKE FIX
            command.ObjectPositionLocal = transform.position;
            command.ObjectRotationLocal = transform.rotation;
        }

        command.GlobalObjectIdHash = HostCheckForGlobalObjectIdHashOverride();
        // todo: check if (IncludeTransformWhenSpawning == null || IncludeTransformWhenSpawning(clientId)) for any clientId
        command.ObjectScale = transform.localScale;

        return command;
    }

    private void SnapshotSpawn()
    {
        var command = GetSpawnCommand();
        //Debug.Log($"PICKUPABLE SPAWN DEBUG -- {nameof(NetworkObject)}.{nameof(SnapshotSpawn)} for {gameObject.Path()} (NO #{command.NetworkObjectId}) create a spawn command with position {command.ObjectPositionLocal} in relation to {(command.ParentNetworkId == command.NetworkObjectId ? "WORLD (has no parent)" : $"parent NO #{command.ParentNetworkId}")}");
        // KEEPSAKE FIX - only spawn for clients that this object passed visibility check for, other clients should get it spawned when it becomes visible to them, via NetworkShow
        //                if not this will simply bypass visibility check and never update Observers, so that clients will receive an additional Spawn command when object actually becomes visible
        var targetClientIds = ListPool<ulong>.Get();

        foreach (var clientId in Observers)
        {
            if (clientId != NetworkManager.ServerClientId)
            {
                targetClientIds.Add(clientId);
            }
        }

        if (targetClientIds.Count > 0)
        {
            // takes ownership of targetClientIds
            NetworkManager.SnapshotSystem.Spawn(command, this, targetClientIds);
        }
        else
        {
            ListPool<ulong>.Release(targetClientIds);
        }
    }

    private void SnapshotSpawn(ulong clientId)
    {
        var command = GetSpawnCommand();

        // KEEPSAKE FIX - use ListPool
        var targetClientIds = ListPool<ulong>.Get();
        targetClientIds.Add(clientId);

        // takes ownership of targetClientIds
        NetworkManager.SnapshotSystem.Spawn(command, this, targetClientIds);
    }

    internal void SnapshotDespawn()
    {
        var command = GetDespawnCommand();
        NetworkManager.SnapshotSystem.Despawn(command, this, null);
    }

    internal void SnapshotDespawn(ulong clientId)
    {
        var command = GetDespawnCommand();
        var targetClientIds = ListPool<ulong>.Get();
        targetClientIds.Add(clientId);
        // takes ownership of targetClientIds
        NetworkManager.SnapshotSystem.Despawn(command, this, targetClientIds);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SpawnInternal(bool destroyWithScene, ulong? ownerClientId, bool playerObject)
    {
        if (!NetworkManager.IsListening)
        {
            throw new NotListeningException($"{nameof(NetworkManager)} is not listening, start a server or host before spawning objects");
        }

        if (!NetworkManager.IsServer)
        {
            throw new NotServerException($"Only server can spawn {nameof(NetworkObject)}s");
        }

        // KEEPSAKE FIX - nested NetworkObjects
        if (IsNested)
        {
            throw new SpawnStateException($"Nested object {gameObject.Path(true)} shouldn't have Spawn called on it. It will get spawned as part of root object {RootNetworkObjectId}.");
        }

        NetworkManager.SpawnManager.SpawnNetworkObjectLocally(this, NetworkManager.SpawnManager.GetNetworkObjectId(), false, playerObject, ownerClientId, destroyWithScene);

        SnapshotSpawn();

        // KEEPSAKE FIX - nested NetworkObjects
        foreach (var nested in GetComponentsInChildren<NetworkObject>(true))
        {
            if (nested.IsNested && nested.RootNetworkObjectId == NetworkObjectId)
            {
                nested.SnapshotSpawn();
            }
        }
    }

    /// <summary>
    ///     Spawns this <see cref="NetworkObject" /> across the network. Can only be called from the Server
    /// </summary>
    /// <param name="destroyWithScene">Should the object be destroyed when the scene is changed</param>
    public void Spawn(bool destroyWithScene = false)
    {
        SpawnInternal(destroyWithScene, null, false);
    }

    /// <summary>
    ///     Spawns a <see cref="NetworkObject" /> across the network with a given owner. Can only be called from server
    /// </summary>
    /// <param name="clientId">The clientId to own the object</param>
    /// <param name="destroyWithScene">Should the object be destroyed when the scene is changed</param>
    public void SpawnWithOwnership(ulong clientId, bool destroyWithScene = false)
    {
        SpawnInternal(destroyWithScene, clientId, false);
    }

    /// <summary>
    ///     Spawns a <see cref="NetworkObject" /> across the network and makes it the player object for the given client
    /// </summary>
    /// <param name="clientId">The clientId whos player object this is</param>
    /// <param name="destroyWithScene">Should the object be destroyd when the scene is changed</param>
    public void SpawnAsPlayerObject(ulong clientId, bool destroyWithScene = false)
    {
        SpawnInternal(destroyWithScene, clientId, true);
    }

    /// <summary>
    ///     Despawns the <see cref="GameObject" /> of this <see cref="NetworkObject" /> and sends a destroy message for it to
    ///     all connected clients.
    /// </summary>
    /// <param name="destroy">
    ///     (true) the <see cref="GameObject" /> will be destroyed (false) the <see cref="GameObject" /> will
    ///     persist after being despawned
    /// </param>
    public void Despawn(bool destroy = true)
    {
        NetworkManager.SpawnManager.DespawnObject(this, destroy);
    }

    /// <summary>
    ///     Removes all ownership of an object from any client. Can only be called from server
    /// </summary>
    public void RemoveOwnership()
    {
        // KEEPSAKE FIX - seen as null when shutting down
        if (NetworkManager == null || NetworkManager.SpawnManager == null)
        {
            return;
        }

        NetworkManager.SpawnManager.RemoveOwnership(this);
    }

    /// <summary>
    ///     Changes the owner of the object. Can only be called from server
    /// </summary>
    /// <param name="newOwnerClientId">The new owner clientId</param>
    public void ChangeOwnership(ulong newOwnerClientId)
    {
        // KEEPSAKE FIX - seen as null when shutting down
        if (NetworkManager == null || NetworkManager.SpawnManager == null)
        {
            return;
        }

        NetworkManager.SpawnManager.ChangeOwnership(this, newOwnerClientId);
    }

    internal void InvokeBehaviourOnLostOwnership()
    {
        for (var i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            // KEEPSAKE FIX - INetworkRpcHandler in ChildNetworkBehaviours
            var (behaviour, _) = ChildNetworkBehaviours[i];
            if (behaviour == null)
            {
                continue;
            }

            behaviour.OnLostOwnership();
        }
    }

    internal void InvokeBehaviourOnGainedOwnership()
    {
        for (var i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            // KEEPSAKE FIX - INetworkRpcHandler in ChildNetworkBehaviours
            var (behaviour, _) = ChildNetworkBehaviours[i];
            if (behaviour == null)
            {
                continue;
            }

            behaviour.OnGainedOwnership();
        }
    }

    // KEEPSAKE FIX - will be invoked for everyone (that is aware) and not only new/old owner
    internal void InvokeBehaviourOnOwnershipChanged(ulong oldOwner, ulong newOwner)
    {
        for (var i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            // KEEPSAKE FIX - INetworkRpcHandler in ChildNetworkBehaviours
            var (behaviour, _) = ChildNetworkBehaviours[i];
            if (behaviour == null)
            {
                continue;
            }

            behaviour.OnOwnershipChanged(oldOwner, newOwner);
        }
    }
    // END KEEPSAKE FIX

    internal void InvokeBehaviourOnNetworkObjectParentChanged(NetworkObject parentNetworkObject)
    {
        for (var i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            // KEEPSAKE FIX - INetworkRpcHandler in ChildNetworkBehaviours
            var (behaviour, _) = ChildNetworkBehaviours[i];
            if (behaviour == null)
            {
                continue;
            }

            behaviour.OnNetworkObjectParentChanged(parentNetworkObject);
        }
    }

    internal void SetCachedParent(Transform parentTransform)
    {
        m_CachedParent = parentTransform;
    }

    internal (bool IsReparented, ulong? LatestParent) GetNetworkParenting()
    {
        return (m_IsReparented, m_LatestParent);
    }

    internal void SetNetworkParenting(bool isReparented, ulong? latestParent)
    {
        m_IsReparented = isReparented;
        m_LatestParent = latestParent;
    }

    public bool TrySetParent(Transform parent, bool worldPositionStays = true)
    {
        return TrySetParent(parent.GetComponent<NetworkObject>(), worldPositionStays);
    }

    public bool TrySetParent(GameObject parent, bool worldPositionStays = true)
    {
        return TrySetParent(parent.GetComponent<NetworkObject>(), worldPositionStays);
    }

    public bool TrySetParent(NetworkObject parent, bool worldPositionStays = true)
    {
        if (!AutoObjectParentSync)
        {
            return false;
        }

        if (NetworkManager == null || !NetworkManager.IsListening)
        {
            return false;
        }

        if (!NetworkManager.IsServer)
        {
            return false;
        }

        // KEEPSAKE FIX - check IsAttached and not IsSpawned
        if (!IsAttached)
        {
            return false;
        }

        if (parent == null)
        {
            return false;
        }

        // KEEPSAKE FIX - check IsAttached and not IsSpawned
        if (!parent.IsAttached)
        {
            return false;
        }

        transform.SetParent(parent.transform, worldPositionStays);
        return true;
    }

    private void OnTransformParentChanged()
    {
        // KEEPSAKE FIX - still track parent even though we still sync ourselves, so that spawn commands etc can be correct
        /*if (!AutoObjectParentSync)
        {
            return;
        }*/

        if (transform.parent == m_CachedParent)
        {
            return;
        }

        if (NetworkManager == null || !NetworkManager.IsListening)
        {
            // KEEPSAKE FIX - track parent but don't enforce
            //transform.parent = m_CachedParent;
            //Debug.LogException(new NotListeningException($"{nameof(NetworkManager)} is not listening, start a server or host before reparenting"));
            return;
        }

        if (!NetworkManager.IsServer)
        {
            // KEEPSAKE FIX - track parent but don't enforce
            //transform.parent = m_CachedParent;
            //Debug.LogException(new NotServerException($"Only the server can reparent {nameof(NetworkObject)}s"));
            return;
        }

        // KEEPSAKE FIX - check IsAttached and not IsSpawned
        if (!IsAttached)
        {
            // KEEPSAKE FIX - track parent but don't enforce
            //transform.parent = m_CachedParent;
            //Debug.LogException(new SpawnStateException($"{nameof(NetworkObject)} can only be reparented after being attached"));
            LogNetcode.Error($"Cannot reparent NetworkObject {gameObject.Path(true)} because it is not spawned with the network layer. If {gameObject.name} was just instantiated, it is probably too early in its lifetime. Try doing this later, from e.g. NetworkSetup or SafeStart.");
            return;
        }

        var parentTransform = transform.parent;
        if (parentTransform != null)
        {
            var parentObject = transform.parent.GetComponent<NetworkObject>();
            if (parentObject == null)
            {
                // KEEPSAKE FIX - track parent but don't enforce
                //transform.parent = m_CachedParent;
                //Debug.LogException(new InvalidParentException($"Invalid parenting, {nameof(NetworkObject)} moved under a non-{nameof(NetworkObject)} parent"));
                LogNetcode.Error($"Cannot reparent NetworkObject {gameObject.Path(true)} to {transform.parent.gameObject.Path(true)} because {transform.parent.gameObject.name} is not a network object.");
                return;
            }

            // KEEPSAKE FIX - check IsAttached and not IsSpawned
            if (!parentObject.IsAttached)
            {
                // KEEPSAKE FIX - track parent but don't enforce
                //transform.parent = m_CachedParent;
                //Debug.LogException(new SpawnStateException($"{nameof(NetworkObject)} can only be reparented under another attached {nameof(NetworkObject)}"));
                LogNetcode.Error($"Cannot reparent NetworkObject {gameObject.Path(true)} to {parentObject.gameObject.Path(true)} because the new parent {parentObject.gameObject.name} is not spawned with the network layer. If it was just instantiated, it is probably too early in its lifetime. Try doing this later, after the parent has run e.g. NetworkSetup or SafeStart.");
                return;
            }

            m_LatestParent = parentObject.NetworkObjectId;
        }
        else
        {
            m_LatestParent = null;
        }

        m_IsReparented = true;
        ApplyNetworkParenting();

        // KEEPSAKE FIX - still track parent even though we still sync ourselves, so that spawn commands etc can be correct
        //                .. now we've reached the sync part
        if (!AutoObjectParentSync)
        {
            return;
        }
        // END KEEPSAKE FIX

        var message = new ParentSyncMessage
        {
            NetworkObjectId = NetworkObjectId,
            IsReparented = m_IsReparented,
            IsLatestParentSet = m_LatestParent != null && m_LatestParent.HasValue,
            LatestParent = m_LatestParent,
        };

        unsafe
        {
            var maxCount = NetworkManager.ConnectedClientsIds.Count;
            var clientIds = stackalloc ulong[maxCount];
            var idx = 0;
            foreach (var clientId in NetworkManager.ConnectedClientsIds)
            {
                if (Observers.Contains(clientId))
                {
                    clientIds[idx++] = clientId;
                }
            }

            NetworkManager.SendMessage(ref message, NetworkDelivery.ReliableSequenced, clientIds, idx);
        }
    }

    // KEEPSAKE FIX - added isSpawning
    internal bool ApplyNetworkParenting(bool isSpawning = false)
    {
        // KEEPSAKE FIX - still track parent even though we still sync ourselves, so that spawn commands etc can be correct
        /*
        if (!AutoObjectParentSync)
        {
            return false;
        }*/

        // KEEPSAKE FIX - check IsAttached and not IsSpawned
        if (!IsAttached)
        {
            return false;
        }

        if (!m_IsReparented)
        {
            return true;
        }

        if (m_LatestParent == null || !m_LatestParent.HasValue)
        {
            m_CachedParent = null;
            // KEEPSAKE FIX - track but don't enforce
            //transform.parent = null;

            InvokeBehaviourOnNetworkObjectParentChanged(null);
            return true;
        }

        // KEEPSAKE FIX - check Attached instead of Spawned
        if (!NetworkManager.SpawnManager.AttachedObjects.ContainsKey(m_LatestParent.Value))
        {
            if (OrphanChildren.Add(this))
            {
                if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
                {
                    NetworkLog.LogWarning($"{nameof(NetworkObject)} ({name}) cannot find its parent, added to {nameof(OrphanChildren)} set");
                }
            }
            return false;
        }

        // KEEPSAKE FIX - read from Attached and not Spawned
        var parentObject = NetworkManager.SpawnManager.AttachedObjects[m_LatestParent.Value];

        m_CachedParent = parentObject.transform;
        // KEEPSAKE FIX - track but don't enforce, but verify
        // transform.parent = parentObject.transform;
        // KEEPSAKE FIX - *do* set if this is part of a spawn on a client, because in this case Netcode is correct and quicker than our usual means
        if (NetworkManager.IsClient && isSpawning)
        {
            transform.parent = parentObject.transform;
        }
        if (parentObject.transform != transform.parent)
        {
            LogNetcode.Error($"Desync between Netcode tracked parent and actual parent of NO #{NetworkObjectId} {gameObject.Path(true)}. Netcode thinks it is {parentObject.gameObject.Path(true)}");
            return false;
        }
        // END KEEPSAKE FIX

        InvokeBehaviourOnNetworkObjectParentChanged(parentObject);
        return true;
    }

    internal static void CheckOrphanChildren()
    {
        var objectsToRemove = new List<NetworkObject>();
        foreach (var orphanObject in OrphanChildren)
        {
            if (orphanObject.ApplyNetworkParenting())
            {
                objectsToRemove.Add(orphanObject);
            }
        }
        foreach (var networkObject in objectsToRemove)
        {
            OrphanChildren.Remove(networkObject);
        }
    }

    // KEEPSAKE FIX
    internal void InvokeBehaviourOnNetworkObjectIdAssigned()
    {
        foreach (var (behaviour, _) in ChildNetworkBehaviours)
        {
            if (behaviour != null)
            {
                behaviour.OnNetworkObjectIdAssigned();
            }
        }
    }

    // KEEPSAKE FIX
    internal void InvokeBehaviourNetworkAttach()
    {
        for (var i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            // KEEPSAKE FIX - INetworkRpcHandler in ChildNetworkBehaviours
            var (behaviour, _) = ChildNetworkBehaviours[i];
            if (behaviour == null)
            {
                continue;
            }

            behaviour.InternalOnNetworkAttach();
        }

        // This isn't beautiful but calling it from NetworkSpawnManager didn't cut it because we really want the net vars to be initialized but we don't want Bind on the KeepsakeNetworkObject to be called since that might trigger SafeStart and all that jazz
        NetworkManager.SpawnManager.m_ObjectAttachedSubject.OnNext(this);

        for (var i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            // KEEPSAKE FIX - INetworkRpcHandler in ChildNetworkBehaviours
            var (behaviour, _) = ChildNetworkBehaviours[i];
            if (behaviour == null)
            {
                continue;
            }

            behaviour.OnNetworkAttach();
        }
    }

    internal void InvokeBehaviourNetworkSpawn()
    {
        for (var i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            // KEEPSAKE FIX - INetworkRpcHandler in ChildNetworkBehaviours
            var (behaviour, _) = ChildNetworkBehaviours[i];
            if (behaviour == null)
            {
                continue;
            }

            behaviour.InternalOnNetworkSpawn();
            behaviour.OnNetworkSpawn();
        }
    }

    internal void InvokeBehaviourNetworkDespawn()
    {
        for (var i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            // KEEPSAKE FIX - INetworkRpcHandler in ChildNetworkBehaviours
            var (behaviour, _) = ChildNetworkBehaviours[i];
            if (behaviour == null)
            {
                continue;
            }

            behaviour.InternalOnNetworkDespawn();
            behaviour.OnNetworkDespawn();
        }
    }

    // KEEPSAKE FIX - made public
    public void WriteNetworkVariableData(FastBufferWriter writer, ulong clientId)
    {
        for (var i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            // KEEPSAKE FIX - INetworkRpcHandler in ChildNetworkBehaviours
            var (behaviour, _) = ChildNetworkBehaviours[i];
            if (behaviour == null)
            {
                continue;
            }

            behaviour.InitializeVariables();
            behaviour.WriteNetworkVariableData(writer, clientId);
        }
    }

    // KEEPSAKE FIX - per-client dirty flags
    internal void MarkVariablesDirty(ulong clientId)
    {
        for (var i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            // KEEPSAKE FIX - INetworkRpcHandler in ChildNetworkBehaviours
            var (behavior, _) = ChildNetworkBehaviours[i];
            if (behavior == null)
            {
                continue;
            }

            behavior.MarkVariablesDirty(clientId);
        }
    }

    // KEEPSAKE FIX - made public
    // KEEPSAKE FIX - added tickRead param since we now call this when handling Spawn commands and want proper tracking
    public void SetNetworkVariableData(FastBufferReader reader, int? tickRead = null)
    {
        for (var i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            // KEEPSAKE FIX - INetworkRpcHandler in ChildNetworkBehaviours
            var (behaviour, _) = ChildNetworkBehaviours[i];
            if (behaviour == null)
            {
                continue;
            }

            behaviour.InitializeVariables();
            behaviour.SetNetworkVariableData(reader, tickRead);
        }
    }

    internal ushort GetNetworkBehaviourOrderIndex(NetworkBehaviour instance)
    {
        // read the cached index, and verify it first
        if (instance.NetworkBehaviourIdCache < ChildNetworkBehaviours.Count)
        {
            if (ChildNetworkBehaviours[instance.NetworkBehaviourIdCache].Item1 == instance)
            {
                return instance.NetworkBehaviourIdCache;
            }

            // invalid cached id reset
            instance.NetworkBehaviourIdCache = default;
        }

        for (ushort i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            if (ChildNetworkBehaviours[i].Item1 == instance)
            {
                // cache the id, for next query
                instance.NetworkBehaviourIdCache = i;
                return i;
            }
        }

        return 0;
    }

    // KEEPSAKE FIX - include implementers of INetworkRpcHandler in id range
    public ushort GetNetworkBehaviourOrderIndex(INetworkRpcHandler instance)
    {
        // read the cached index, and verify it first
        if (instance.NetworkBehaviourIdCache < ChildNetworkBehaviours.Count)
        {
            if (ChildNetworkBehaviours[instance.NetworkBehaviourIdCache].Item2 == instance)
            {
                return instance.NetworkBehaviourIdCache;
            }

            // invalid cached id reset
            instance.NetworkBehaviourIdCache = default;
        }

        for (ushort i = 0; i < ChildNetworkBehaviours.Count; i++)
        {
            if (ChildNetworkBehaviours[i].Item2 == instance)
            {
                // cache the id, for next query
                instance.NetworkBehaviourIdCache = i;
                return i;
            }
        }

        return 0;
    }

    internal NetworkBehaviour GetNetworkBehaviourAtOrderIndex(ushort index)
    {
        if (index >= ChildNetworkBehaviours.Count)
        {
            if (NetworkLog.CurrentLogLevel <= LogLevel.Error)
            {
                NetworkLog.LogError($"Behaviour index was out of bounds. Did you mess up the order of your {nameof(NetworkBehaviour)}s?");
            }

            return null;
        }

        return ChildNetworkBehaviours[index].Item1;
    }

    // KEEPSAKE FIX - include implementers of INetworkRpcHandler in id range
    internal INetworkRpcHandler GetRpcHandlerAtOrderIndex(ushort index)
    {
        if (index >= ChildNetworkBehaviours.Count)
        {
            if (NetworkLog.CurrentLogLevel <= LogLevel.Error)
            {
                NetworkLog.LogError($"Behaviour index was out of bounds. Did you mess up the order of your {nameof(NetworkBehaviour)}s?");
            }

            return null;
        }

        return ChildNetworkBehaviours[index].Item2;
    }

    // KEEPSAKE FIX - make public
    public SceneObject GetMessageSceneObject(ulong targetClientId, bool includeNetworkVariableData = true)
    {
        var obj = new SceneObject
        {
            Header = new SceneObject.HeaderData
            {
                IsPlayerObject = IsPlayerObject,
                NetworkObjectId = NetworkObjectId,
                OwnerClientId = OwnerClientId,
                IsSceneObject = IsSceneObject ?? true,
                Hash = HostCheckForGlobalObjectIdHashOverride(),
                HasNetworkVariables = includeNetworkVariableData,
            },
            OwnerObject = this,
            TargetClientId = targetClientId,
        };

        if (IsNested)
        {
            obj.Header.IsNested = true;
            obj.SpawnedParentObjectId = RootNetworkObjectId;
        }

        NetworkObject parentNetworkObject = null;

        if (!AlwaysReplicateAsRoot && transform.parent != null)
        {
            parentNetworkObject = transform.parent.GetComponent<NetworkObject>();
        }

        if (parentNetworkObject)
        {
            obj.Header.HasParent = true;
            obj.Header.WaitForParentIfMissing = parentNetworkObject.IsAttached && !parentNetworkObject.IsSpawned;
            obj.ParentObjectId = parentNetworkObject.NetworkObjectId;
        }
        if (IncludeTransformWhenSpawning == null || IncludeTransformWhenSpawning(OwnerClientId))
        {
            obj.Header.HasTransform = true;
            obj.Transform = new SceneObject.TransformData
            {
                // KEEPSAKE FIX - if has parent, send in "parent space"
                Position = parentNetworkObject != null ? transform.localPosition : transform.position,
                Rotation = parentNetworkObject != null ? transform.localRotation : transform.rotation,
            };
        }

        var (isReparented, latestParent) = GetNetworkParenting();
        obj.Header.IsReparented = isReparented;
        if (isReparented)
        {
            var isLatestParentSet = latestParent != null && latestParent.HasValue;
            obj.IsLatestParentSet = isLatestParentSet;
            if (isLatestParentSet)
            {
                obj.LatestParent = latestParent.Value;
            }
        }

        return obj;
    }

    /// <summary>
    ///     Used to deserialize a serialized scene object which occurs
    ///     when the client is approved or during a scene transition
    /// </summary>
    /// <param name="sceneObject">Deserialized scene object data</param>
    /// <param name="variableData">reader for the NetworkVariable data</param>
    /// <param name="networkManager">NetworkManager instance</param>
    /// <returns>optional to use NetworkObject deserialized</returns>
    internal static async UniTask<NetworkObject> AddSceneObjectAsync(SceneObject sceneObject, FastBufferReader variableData, NetworkManager networkManager)
    {
        Vector3? positionParentSpace = null;
        Quaternion? rotationParentSpace = null;
        ulong? parentNetworkId = null;
        ulong? spawnedParentNetworkid = null; // KEEPSAKE FIX - nesting

        if (sceneObject.Header.HasTransform)
        {
            positionParentSpace = sceneObject.Transform.Position;
            rotationParentSpace = sceneObject.Transform.Rotation;
        }

        if (sceneObject.Header.HasParent)
        {
            parentNetworkId = sceneObject.ParentObjectId;
        }

        // KEEPSAKE FIX - nesting
        if (sceneObject.Header.IsNested)
        {
            spawnedParentNetworkid = sceneObject.SpawnedParentObjectId;
        }

        //Attempt to create a local NetworkObject
        var networkObject = await networkManager.SpawnManager.CreateLocalNetworkObjectAsync(sceneObject.Header.IsSceneObject, sceneObject.Header.Hash, sceneObject.Header.OwnerClientId, parentNetworkId, spawnedParentNetworkid, sceneObject.Header.WaitForParentIfMissing, positionParentSpace, rotationParentSpace, sceneObject.Header.IsReparented);

        if (networkObject == null)
        {
            // Log the error that the NetworkObject failed to construct
            Debug.LogError($"Failed to spawn {nameof(NetworkObject)} for Hash {sceneObject.Header.Hash}.");

            // KEEPSAKE FIX - there might not be any more data to read, so don't try to read varSize if that is the case
            if (variableData.Length - variableData.Position > sizeof(int))
            {
                // If we failed to load this NetworkObject, then skip past the network variable data
                variableData.ReadValueSafe(out int size);
                variableData.Seek(variableData.Position + size);
            }

            // We have nothing left to do here.
            return null;
        }

        networkObject.SetNetworkParenting(sceneObject.Header.IsReparented, sceneObject.LatestParent);

        // Spawn the NetworkObject(
        networkManager.SpawnManager.SpawnNetworkObjectLocally(networkObject, sceneObject, variableData, false);

        return networkObject;
    }

    /// <summary>
    ///     Only applies to Host mode.
    ///     Will return the registered source NetworkPrefab's GlobalObjectIdHash if one exists.
    ///     Server and Clients will always return the NetworkObject's GlobalObjectIdHash.
    /// </summary>
    /// <returns></returns>
    internal uint HostCheckForGlobalObjectIdHashOverride()
    {
        if (NetworkManager.IsHost)
        {
            if (NetworkManager.PrefabHandler.ContainsHandler(this))
            {
                var globalObjectIdHash = NetworkManager.PrefabHandler.GetSourceGlobalObjectIdHash(GlobalObjectIdHash);
                return globalObjectIdHash == 0 ? GlobalObjectIdHash : globalObjectIdHash;
            }
            else if (NetworkManager.NetworkConfig.OverrideToNetworkPrefab.ContainsKey(GlobalObjectIdHash))
            {
                return NetworkManager.NetworkConfig.OverrideToNetworkPrefab[GlobalObjectIdHash];
            }
        }

        return GlobalObjectIdHash;
    }

    // KEEPSAKE FIX - make public
    public struct SceneObject
    {
        #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
        private static readonly  byte[] k_MarkerObjectBegin   = { 0xfe, 0xed, 0xf0, 0x0d, 0x01 };
        private static readonly  byte[] k_MarkerObjectEnd     = { 0xfe, 0xed, 0xf0, 0x0d, 0x02 };
        internal static readonly byte[] MarkerObjectEndVars = { 0xfe, 0xed, 0xf0, 0x0d, 0x03 };
        #endif

        public struct HeaderData
        {
            public ulong NetworkObjectId;
            public ulong OwnerClientId;
            public uint  Hash;

            public bool IsPlayerObject;
            public bool HasParent;
            public bool WaitForParentIfMissing; // KEEPSAKE FIX
            public bool IsSceneObject;
            public bool HasTransform;
            public bool IsReparented;
            public bool HasNetworkVariables;

            // KEEPSAKE FIX
            public bool IsNested;
        }

        public HeaderData Header;

        //If(Metadata.HasParent)
        public ulong ParentObjectId;

        // KEEPSAKE FIX - nesting
        //If(Metadata.IsNested)
        public ulong SpawnedParentObjectId;

        //If(Metadata.HasTransform)
        public struct TransformData
        {
            public Vector3    Position;
            public Quaternion Rotation;
        }

        public TransformData Transform;

        //If(Metadata.IsReparented)
        public bool IsLatestParentSet;

        //If(IsLatestParentSet)
        public ulong? LatestParent;

        public NetworkObject OwnerObject;
        public ulong         TargetClientId;

        public unsafe void Serialize(FastBufferWriter writer)
        {
            // KEEPSAKE FIX - boundary markers
            #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
            if (!writer.TryBeginWrite(FastBufferWriter.GetWriteSize<int>() + k_MarkerObjectBegin.Length))
            {
                throw new OverflowException("Could not serialize SceneObject begin marker: Out of buffer space.");
            }

            writer.WriteValue(k_MarkerObjectBegin.Length);
            foreach (var b in k_MarkerObjectBegin)
            {
                writer.WriteValue(b);
            }
            #endif

            if (!writer.TryBeginWrite(sizeof(HeaderData) + (Header.HasParent ? FastBufferWriter.GetWriteSize(ParentObjectId) : 0) + (Header.IsNested ? FastBufferWriter.GetWriteSize(SpawnedParentObjectId) : 0) + (Header.HasTransform ? FastBufferWriter.GetWriteSize(Transform) : 0) + (Header.IsReparented ? FastBufferWriter.GetWriteSize(IsLatestParentSet) + (IsLatestParentSet ? FastBufferWriter.GetWriteSize<ulong>() : 0) : 0)))
            {
                throw new OverflowException("Could not serialize SceneObject: Out of buffer space.");
            }

            writer.WriteValue(Header);

            if (Header.HasParent)
            {
                writer.WriteValue(ParentObjectId);
            }

            // KEEPSAKE FIX - nesting
            if (Header.IsNested)
            {
                writer.WriteValue(SpawnedParentObjectId);
            }

            if (Header.HasTransform)
            {
                writer.WriteValue(Transform);
            }

            if (Header.IsReparented)
            {
                writer.WriteValue(IsLatestParentSet);
                if (IsLatestParentSet)
                {
                    writer.WriteValue((ulong)LatestParent);
                }
            }

            // KEEPSAKE FIX - boundary markers
            #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
            if (!writer.TryBeginWrite(FastBufferWriter.GetWriteSize<int>() + k_MarkerObjectEnd.Length))
            {
                throw new OverflowException("Could not serialize SceneObject end marker: Out of buffer space.");
            }

            writer.WriteValue(k_MarkerObjectEnd.Length);
            foreach (var b in k_MarkerObjectEnd)
            {
                writer.WriteValue(b);
            }
            #endif

            if (Header.HasNetworkVariables)
            {
                // KEEPSAKE FIX - write entire variable data length
                var writePos = writer.Position;
                writer.WriteValueSafe(0);
                var startPos = writer.Position;
                // END KEEPSAKE FIX

                OwnerObject.WriteNetworkVariableData(writer, TargetClientId);

                // KEEPSAKE FIX - write entire variable data length
                var size = writer.Position - startPos;
                writer.Seek(writePos);
                writer.WriteValueSafe(size);
                writer.Seek(startPos + size);
                // END KEEPSAKE FIX

                // KEEPSAKE FIX - boundary markers
                #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
                if (!writer.TryBeginWrite(FastBufferWriter.GetWriteSize<int>() + MarkerObjectEndVars.Length))
                {
                    throw new OverflowException("Could not serialize SceneObject end vars marker: Out of buffer space.");
                }

                writer.WriteValue(MarkerObjectEndVars.Length);
                foreach (var b in MarkerObjectEndVars)
                {
                    writer.WriteValue(b);
                }
                #endif
            }
        }

        public unsafe void Deserialize(FastBufferReader reader)
        {
            #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
            if (!reader.TryBeginRead(sizeof(int) + k_MarkerObjectBegin.Length))
            {
                throw new OverflowException($"Could not deserialize SceneObject begin marker: Ran out of data (at byte {reader.Position} of {reader.Length}).");
            }
            reader.ReadValue(out int markerLength);
            Assert.AreEqual(k_MarkerObjectBegin.Length, markerLength, "NetworkObject begin marker should be of expected length");
            for (var i = 0; i < markerLength; ++i)
            {
                reader.ReadValue(out byte actualByte);
                Assert.AreEqual(k_MarkerObjectBegin[i], actualByte, $"NetworkObject begin marker should be intact (byte {i})");
            }
            #endif

            if (!reader.TryBeginRead(sizeof(HeaderData)))
            {
                throw new OverflowException($"Could not deserialize SceneObject header ({sizeof(HeaderData)} bytes): Ran out of data (at byte {reader.Position} of {reader.Length}).");
            }
            reader.ReadValue(out Header);
            var bytesToRead = (Header.HasParent ? FastBufferWriter.GetWriteSize(ParentObjectId) : 0)
                + (Header.IsNested ? FastBufferWriter.GetWriteSize(SpawnedParentObjectId) : 0) // KEEPSAKE FIX - nesting
                + (Header.HasTransform ? FastBufferWriter.GetWriteSize(Transform) : 0)
                + (Header.IsReparented ? FastBufferWriter.GetWriteSize(IsLatestParentSet) : 0);
            if (!reader.TryBeginRead(bytesToRead))
            {
                throw new OverflowException($"Could not deserialize SceneObject ({bytesToRead} bytes): Ran out of data (at byte {reader.Position} of {reader.Length}).");
            }

            if (Header.HasParent)
            {
                reader.ReadValue(out ParentObjectId);
            }

            if (Header.IsNested)
            {
                reader.ReadValue(out SpawnedParentObjectId);
            }

            if (Header.HasTransform)
            {
                reader.ReadValue(out Transform);
            }

            if (Header.IsReparented)
            {
                reader.ReadValue(out IsLatestParentSet);
                if (IsLatestParentSet)
                {
                    reader.ReadValueSafe(out ulong latestParent);
                    LatestParent = latestParent;
                }
            }

            #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
            if (!reader.TryBeginRead(sizeof(int) + k_MarkerObjectEnd.Length))
            {
                throw new OverflowException($"Could not deserialize SceneObject {Header.NetworkObjectId} / {Header.Hash} end marker: Ran out of data (at byte {reader.Position} of {reader.Length}).");
            }
            reader.ReadValue(out markerLength);
            Assert.AreEqual(k_MarkerObjectEnd.Length, markerLength, "NetworkObject end marker should be of expected length");
            for (var i = 0; i < markerLength; ++i)
            {
                reader.ReadValue(out byte actualByte);
                Assert.AreEqual(k_MarkerObjectEnd[i], actualByte, $"NetworkObject end marker should be intact (byte {i})");
            }
            #endif
        }
    }

    #if UNITY_EDITOR

    private void OnValidate()
    {
        RegenerateGlobalObjectIdHash();
    }

    // KEEPSAKE FIX - made public to call from our migration assistant
    public void RegenerateGlobalObjectIdHash(bool force = false, bool isSaving = false)
    {
        // KEEPSAKE FIX - lets not ever regenerate during play, been seeing OnValidate called on NetworkObjects in invalid scenes, which causes an error...
        if (EditorApplication.isPlaying)
        {
            return;
        }

        // do NOT regenerate GlobalObjectIdHash for NetworkPrefabs while Editor is in PlayMode
        if (EditorApplication.isPlaying && !string.IsNullOrEmpty(gameObject.scene.name))
        {
            return;
        }

        // do NOT regenerate GlobalObjectIdHash if Editor is transitioning into or out of PlayMode
        if (!EditorApplication.isPlaying && EditorApplication.isPlayingOrWillChangePlaymode)
        {
            return;
        }

        // KEEPSAKE FIX - do NOT regenerate when importing assets, asset import worker is running in batch mode, so check that too
        if (EditorApplication.isUpdating || Application.isBatchMode)
        {
            return;
        }

        // KEEPSAKE FIX - prefer addressable key as GlobalObjectId
        // The root is always addressable, and spawned via its addressable key.
        // A root is either an addressable scene that is streamed in, or an addressable networked prefab that is spawned while playing.
        // These can themselves contain networked objects ("scene objects" for scenes, and "nested network objects" for prefabs),
        // and nested objects *never* use the addressable key, since these are instances and there can be multiple of the same type and
        // they all need to have unique GlobalObjectIds. So for example a scene is not allowed to contain multiple network objects with the same ID.
        //
        // The reason we prefer using addressable keys is that it enables us to skip the part where spawnable prefabs must be added to the NetworkManager
        // as "NetworkPrefabs" and instead they can simply be made addressable for it to work (which is done automatically for all prefabs with the NetworkObject component).

        // Here be original Netcode:
        //var globalObjectIdString = UnityEditor.GlobalObjectId.GetGlobalObjectIdSlow(this).ToString();
        //GlobalObjectIdHash = XXHash.Hash32(globalObjectIdString);

        if (NetworkEditorSceneLoadingTracker.IsOpeningScene)
        {
            // OnValidate and dirty state doesn't work while opening the scene, so skip "fixing" during scene load since it'll just cheat the developer.
            return;
        }

        var oldHash = GlobalObjectIdHash;
        if (!TryGenerateGlobalIdHash(this, out var newHash, out var newHashDebugInput))
        {
            // When working with nested network objects in prefab stage it will fail to generate a proper ID until the instance has a valid file ID,
            // which it seems to get when the root prefab gets saved (fair enough), so here we keep trying until prefab gets saved and a valid ID gets generated.
            // If "Auto-Save" is disabled in the prefab stage editor the Save button will need to be clicked twice, first to save the prefab, second to save the now generated ID.
            // If "Auto-Save" is enabled this happens automatically.
            // Not terrible, not great.
            var prefabStage = UnityEditor.SceneManagement.PrefabStageUtility.GetPrefabStage(gameObject);
            if (prefabStage != null)
            {
                EditorApplication.delayCall += () =>
                {
                    if (this != null)
                    {
                        RegenerateGlobalObjectIdHash();
                    }
                };
            }
            else if (isSaving && SceneManager.GetSceneAt(0) == gameObject.scene && string.IsNullOrEmpty(gameObject.scene.path))
            {
                // If saving a spanking new scene we won't get an ID, and we can't get one until the scene has been saved once.
                // If so we'll immediately dirty it and re-save it

                void SaveSceneWithDelay()
                {
                    var scene = SceneManager.GetSceneAt(0);
                    UnityEditor.SceneManagement.EditorSceneManager.SaveScene(scene);
                }

                EditorApplication.delayCall += () =>
                {
                    SaveSceneWithDelay(); // once with delay
                    EditorApplication.delayCall += SaveSceneWithDelay; // once with twice the delay!
                };
            }

            return;
        }

        if (newHash != oldHash || force)
        {
            GlobalObjectIdHash = newHash;
            GlobalObjectIdHash_DebugInput = newHashDebugInput;

            // mark as dirty next Editor frame to avoid any current load scene operation to clear the dirty flag
            EditorApplication.delayCall += () =>
            {
                if (this != null)
                {
                    PrefabUtility.RecordPrefabInstancePropertyModifications(this);
                    EditorUtility.SetDirty(this);
                }
            };
        }
        // END KEEPSAKE FIX
    }

    public static bool TryGenerateGlobalIdHash(NetworkObject networkObject, out uint globalIdHash, out string globalIdHashDebugInput)
    {
        GUID? assetGuid = null;
        bool? isRoot = null;

        var prefabStage = UnityEditor.SceneManagement.PrefabStageUtility.GetPrefabStage(networkObject.gameObject);

        if (PrefabUtility.IsPartOfPrefabAsset(networkObject)) // e.g. prefab selected in Project tab
        {
            var assetPath = AssetDatabase.GetAssetPath(networkObject);
            if (string.IsNullOrEmpty(assetPath))
            {
                // this seems to happen while saving a prefab opened in prefab stage
                globalIdHashDebugInput = default;
                globalIdHash = default;
                return false;
            }

            var asset = AssetDatabase.LoadAssetAtPath<GameObject>(assetPath);
            isRoot = asset == networkObject.gameObject;
            if (isRoot == true)
            {
                assetGuid = AssetDatabase.GUIDFromAssetPath(assetPath);
            }
        }
        else if (prefabStage != null) // check if root of prefab stage
        {
            isRoot = networkObject.gameObject.transform.parent == null;
            if (isRoot == true)
            {
                assetGuid = AssetDatabase.GUIDFromAssetPath(prefabStage.assetPath);
            }
        }
        else if (UnityEditor.SceneManagement.EditorSceneManager.IsPreviewSceneObject(networkObject))
        {
            if (!networkObject.gameObject.scene.IsValid())
            {
                // this has been seen when OnValidate runs as part of a preview scene, probably when the preview scene is opening/loading
                globalIdHashDebugInput = default;
                globalIdHash = default;
                return false;
            }

            isRoot = networkObject.gameObject.transform.parent == null;
            if (isRoot == true)
            {
                // using PrefabUtility.GetPrefabAssetPathOfNearestInstanceRoot might be *wrong* when dealing with a prefab variant and seems to return
                // the path of the base prefab, and not the prefab we currently are operating on.
                // using the scene path seems to work..

                assetGuid = AssetDatabase.GUIDFromAssetPath(networkObject.gameObject.scene.path);
            }
        }
        else if (SceneManager.GetSceneAt(0) == networkObject.gameObject.scene && string.IsNullOrEmpty(networkObject.gameObject.scene.path))
        {
            // When working in an shiny new never-saved Scene we can't generate valid IDs, will be generated on save
            globalIdHashDebugInput = default;
            globalIdHash = default;
            return false;
        }


        if (isRoot == true)
        {
            var settings = AddressableAssetSettingsDefaultObject.Settings;
            var entry = settings.FindAssetEntry(assetGuid.ToString()) ?? settings.CreateOrMoveEntry(assetGuid.ToString(), settings.DefaultGroup);
            entry.SetLabel(AddressableLabel, true, true);
            globalIdHashDebugInput = entry.address;
            globalIdHash = HashingAlgorithm(entry.address);
            return true;
        }

        var globalId = GlobalObjectId.GetGlobalObjectIdSlow(networkObject);

        if (globalId.identifierType == 1)
        {
            // type 1 == "Imported Asset"
            // We don't like this type, it has been seen when Unity is starting up and loading the previously loaded scene, runs OnValidate on the *assets* that scene contains (maybe reasonable)
            // but the File ID part of `globalId` will be the File ID of the *instance* in the *scene* and not of the nested prefab in the asset (which `this` refers to).
            // The result is that the *asset* on disk will get a new global id hash based on the instance in the scene, which is not good since we want these to be stable.
            // We just exit out here and assume a nice "type 2" (Scene Object) ID will come along at some point.
            globalIdHashDebugInput = default;
            globalIdHash = default;
            return false;
        }

        if (prefabStage != null)
        {
            if (globalId.targetObjectId == 0)
            {
                // If we're in a prefab stage (but not root which has already been handled above) we expect to have a local file ID, otherwise
                // our generated ID will be identical to our asset (since what will be hashed is [Asset GUID]-0-0, basically).
                // This might happen when duplicating a nested object in a prefab stage, before the prefab has been saved.
                globalIdHashDebugInput = default;
                globalIdHash = default;
                return false;
            }

            assetGuid = AssetDatabase.GUIDFromAssetPath(prefabStage.assetPath);
            if (!GlobalObjectId.TryParse(
                    globalId.ToString().Replace("00000000000000000000000000000000", assetGuid.ToString()),
                    out globalId))
            {
                throw new Exception($"Unable to construct {nameof(GlobalObjectId)} for prefab stage");
            }
        }

        if (globalId.assetGUID.ToString() == "00000000000000000000000000000000")
        {
            throw new Exception(
                $"Couldn't construct valid {nameof(GlobalObjectId)} for {networkObject.gameObject.Path(true)}. Maybe this is an edge case we must implement support for.");
        }

        globalIdHashDebugInput = globalId.ToString();
        globalIdHash = HashingAlgorithm(globalId.ToString());
        return true;
    }
    #endif
}

}
