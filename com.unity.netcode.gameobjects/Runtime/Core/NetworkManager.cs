using System;
using System.Collections;
using System.Collections.Generic;
using Cysharp.Threading.Tasks;
using UniRx;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Netcode.Interest;
using UnityEngine;
#if UNITY_EDITOR
using UnityEditor;
#endif
#if MULTIPLAYER_TOOLS
using Unity.Multiplayer.Tools;
#endif
using Unity.Profiling;
using UnityEngine.SceneManagement;
using Debug = UnityEngine.Debug;

namespace Unity.Netcode
{

/// <summary>
///     The main component of the library
/// </summary>
[AddComponentMenu("Netcode/" + nameof(NetworkManager), -100)]
public class NetworkManager : MonoBehaviour, INetworkUpdateSystem
{
    #pragma warning disable IDE1006 // disable naming rule violation check

    // RuntimeAccessModifiersILPP will make this `public`
    // KEEPSAKE FIX - support for INetworkRpcHandler as first param
    internal delegate void RpcReceiveHandler(object behaviourOrRpcHandler, FastBufferReader reader, __RpcParams parameters);

    // RuntimeAccessModifiersILPP will make this `public`
    internal static readonly Dictionary<uint, RpcReceiveHandler> __rpc_func_table = new();

    #if DEVELOPMENT_BUILD || UNITY_EDITOR
    // RuntimeAccessModifiersILPP will make this `public`
    internal static readonly Dictionary<uint, string> __rpc_name_table = new();
    #endif

    #pragma warning restore IDE1006 // restore naming rule violation check

    #if DEVELOPMENT_BUILD || UNITY_EDITOR
    private static ProfilerMarker s_SyncTime            = new($"{nameof(NetworkManager)}.SyncTime");
    private static ProfilerMarker s_TransportPoll       = new($"{nameof(NetworkManager)}.TransportPoll");
    private static ProfilerMarker s_TransportConnect    = new($"{nameof(NetworkManager)}.TransportConnect");
    private static ProfilerMarker s_HandleIncomingData  = new($"{nameof(NetworkManager)}.{nameof(HandleIncomingData)}");
    private static ProfilerMarker s_TransportDisconnect = new($"{nameof(NetworkManager)}.TransportDisconnect");
    #endif

    private const double k_TimeSyncFrequency    = 1.0d;  // sync every second, TODO will be removed once timesync is done via snapshots
    private const float  k_DefaultBufferSizeSec = 0.05f; // todo talk with UX/Product, find good default value for this

    // KEEPSAKE FIX - delivery tracking
    public Action<uint> SnapshotUpdateQueued;
    public Action<uint> SnapshotUpdateDelivered;
    public Action<uint> SnapshotSpawnQueued;
    public Action<uint> SnapshotSpawnDelivered;
    public Action<uint> SnapshotAttachQueued;
    public Action<uint> SnapshotAttachDelivered;
    public Action<uint> SnapshotDespawnQueued;
    public Action<uint> SnapshotDespawnDelivered;
    // only tracking snapshot commands for now (not RPCs/messages etc)
    // END KEEPSAKE FIX

    internal static string PrefabDebugHelper(NetworkPrefab networkPrefab)
    {
        return $"{nameof(NetworkPrefab)} \"{networkPrefab.Prefab.gameObject.name}\"";
    }

    // KEEPSAKE FIX
    public object m_SimulationMessageBus;

    private InterestManager<NetworkObject> m_InterestManager;

    // For unit (vs. integration) testing and for better decoupling, we don't want to have to require Initialize()
    //  to use the InterestManager
    public InterestManager<NetworkObject> InterestManager
    {
        get
        {
            if (m_InterestManager == null)
            {
                m_InterestManager = new InterestManager<NetworkObject>();
            }
            return m_InterestManager;
        }
    }

    // KEEPSAKE FIX - made public!
    public   SnapshotSystem          SnapshotSystem   { get; private set; }
    internal NetworkBehaviourUpdater BehaviourUpdater { get; private set; }

    // KEEPSAKE FIX - made public
    public MessagingSystem MessagingSystem { get; private set; }

    private NetworkPrefabHandler m_PrefabHandler;

    public NetworkPrefabHandler PrefabHandler
    {
        get
        {
            if (m_PrefabHandler == null)
            {
                m_PrefabHandler = new NetworkPrefabHandler();
            }

            return m_PrefabHandler;
        }
    }

    private bool m_StopProcessingMessages;

    // KEEPSAKE FIX
    private readonly HashSet<NetworkBehaviour> m_InactiveInitializedBehaviours = new();

    private class NetworkManagerHooks : INetworkHooks
    {
        private readonly NetworkManager m_NetworkManager;

        internal NetworkManagerHooks(NetworkManager manager)
        {
            m_NetworkManager = manager;
        }

        public void OnBeforeSendMessage<T>(ulong clientId, ref T message, NetworkDelivery delivery) where T : INetworkMessage
        {
        }

        public void OnAfterSendMessage<T>(ulong clientId, ref T message, NetworkDelivery delivery, int messageSizeBytes)
            where T : INetworkMessage
        {
        }

        public void OnBeforeReceiveMessage(ulong senderId, Type messageType, int messageSizeBytes)
        {
        }

        public void OnAfterReceiveMessage(ulong senderId, Type messageType, int messageSizeBytes)
        {
        }

        public void OnBeforeSendBatch(ulong clientId, int messageCount, int batchSizeInBytes, NetworkDelivery delivery)
        {
        }

        public void OnAfterSendBatch(ulong clientId, int messageCount, int batchSizeInBytes, NetworkDelivery delivery)
        {
        }

        public void OnBeforeReceiveBatch(ulong senderId, int messageCount, int batchSizeInBytes)
        {
        }

        public void OnAfterReceiveBatch(ulong senderId, int messageCount, int batchSizeInBytes)
        {
        }

        public bool OnVerifyCanSend(ulong destinationId, Type messageType, NetworkDelivery delivery)
        {
            return !m_NetworkManager.m_StopProcessingMessages;
        }

        public bool OnVerifyCanReceive(ulong senderId, Type messageType)
        {
            if (m_NetworkManager.PendingClients.TryGetValue(senderId, out var client)
                && (client.ConnectionState == PendingClient.State.PendingApproval
                    || (client.ConnectionState == PendingClient.State.PendingConnection
                        && messageType != typeof(ConnectionRequestMessage))))
            {
                if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
                {
                    NetworkLog.LogWarning($"Message received from {nameof(senderId)}={senderId.ToString()} before it has been accepted");
                }

                return false;
            }

            return !m_NetworkManager.m_StopProcessingMessages;
        }

        public void OnBeforeHandleMessage<T>(ref T message, ref NetworkContext context) where T : INetworkMessage
        {
        }

        public void OnAfterHandleMessage<T>(ref T message, ref NetworkContext context) where T : INetworkMessage
        {
        }
    }

    private class NetworkManagerMessageSender : IMessageSender
    {
        private readonly NetworkManager m_NetworkManager;

        public NetworkManagerMessageSender(NetworkManager manager)
        {
            m_NetworkManager = manager;
        }

        public void Send(ulong clientId, NetworkDelivery delivery, FastBufferWriter batchData)
        {
            var sendBuffer = batchData.ToTempByteArray();

            m_NetworkManager.NetworkConfig.NetworkTransport.Send(m_NetworkManager.ClientIdToTransportId(clientId), sendBuffer, delivery);
        }
    }

    /// <summary>
    ///     Returns the <see cref="GameObject" /> to use as the override as could be defined within the NetworkPrefab list
    ///     Note: This should be used to create <see cref="GameObject" /> pools (with <see cref="NetworkObject" /> components)
    ///     under the scenario where you are using the Host model as it spawns everything locally. As such, the override
    ///     will not be applied when spawning locally on a Host.
    ///     Related Classes and Interfaces:
    ///     <see cref="NetworkPrefabHandler" />
    ///     <see cref="INetworkPrefabInstanceHandler" />
    /// </summary>
    /// <param name="gameObject">
    ///     the <see cref="GameObject" /> to be checked for a <see cref="NetworkManager" /> defined
    ///     NetworkPrefab override
    /// </param>
    /// <returns>
    ///     a <see cref="GameObject" /> that is either the override or if no overrides exist it returns the same as the
    ///     one passed in as a parameter
    /// </returns>
    public GameObject GetNetworkPrefabOverride(GameObject gameObject)
    {
        var networkObject = gameObject.GetComponent<NetworkObject>();
        if (networkObject != null)
        {
            if (NetworkConfig.NetworkPrefabOverrideLinks.ContainsKey(networkObject.GlobalObjectIdHash))
            {
                switch (NetworkConfig.NetworkPrefabOverrideLinks[networkObject.GlobalObjectIdHash].Override)
                {
                    case NetworkPrefabOverride.Hash:
                    case NetworkPrefabOverride.Prefab:
                    {
                        return NetworkConfig.NetworkPrefabOverrideLinks[networkObject.GlobalObjectIdHash].OverridingTargetPrefab;
                    }
                }
            }
        }
        return gameObject;
    }

    public NetworkTimeSystem NetworkTimeSystem { get; private set; }

    public NetworkTickSystem NetworkTickSystem { get; private set; }

    public NetworkTime LocalTime => NetworkTickSystem?.LocalTime ?? default;

    public NetworkTime ServerTime => NetworkTickSystem?.ServerTime ?? default;

    /// <summary>
    ///     Gets or sets if the application should be set to run in background
    /// </summary>
    [HideInInspector]
    public bool RunInBackground = true;

    /// <summary>
    ///     The log level to use
    /// </summary>
    [HideInInspector]
    public LogLevel LogLevel = LogLevel.Normal;

    /// <summary>
    ///     The singleton instance of the NetworkManager
    /// </summary>
    public static NetworkManager Singleton { get; private set; }

    /// <summary>
    ///     Gets the SpawnManager for this NetworkManager
    /// </summary>
    public NetworkSpawnManager SpawnManager { get; private set; }

    public CustomMessagingManager CustomMessagingManager { get; private set; }

    public NetworkSceneManager SceneManager { get; private set; }

    public readonly ulong ServerClientId = 0;

    /// <summary>
    ///     Gets the networkId of the server
    /// </summary>
    private ulong m_ServerTransportId =>
        NetworkConfig.NetworkTransport?.ServerClientId
        ?? throw new NullReferenceException($"The transport in the active {nameof(NetworkConfig)} is null");

    /// <summary>
    ///     Returns ServerClientId if IsServer or LocalClientId if not
    /// </summary>
    public ulong LocalClientId
    {
        get => IsServer ? NetworkConfig.NetworkTransport.ServerClientId : m_LocalClientId;
        internal set => m_LocalClientId = value;
    }

    private ulong m_LocalClientId;

    private readonly Dictionary<ulong, NetworkClient> m_ConnectedClients = new();

    private          ulong                    m_NextClientId             = 1;
    private readonly Dictionary<ulong, ulong> m_ClientIdToTransportIdMap = new();
    private readonly Dictionary<ulong, ulong> m_TransportIdToClientIdMap = new();

    private readonly List<NetworkClient> m_ConnectedClientsList = new();

    private List<ulong> m_ConnectedClientIds = new();

    // KEEPSAKE FIX
    private NativeArray<ulong> m_ConnectedClientIdsNativeArray;

    /// <summary>
    ///     Gets a dictionary of connected clients and their clientId keys. This is only accessible on the server.
    /// </summary>
    public IReadOnlyDictionary<ulong, NetworkClient> ConnectedClients
    {
        get
        {
            if (IsServer == false)
            {
                throw new NotServerException($"{nameof(ConnectedClients)} should only be accessed on server.");
            }
            return m_ConnectedClients;
        }
    }

    /// <summary>
    ///     Gets a list of connected clients. This is only accessible on the server.
    /// </summary>
    public IReadOnlyList<NetworkClient> ConnectedClientsList
    {
        get
        {
            if (IsServer == false)
            {
                throw new NotServerException($"{nameof(ConnectedClientsList)} should only be accessed on server.");
            }
            return m_ConnectedClientsList;
        }
    }

    /// <summary>
    ///     Gets a list of just the IDs of all connected clients. This is only accessible on the server.
    /// </summary>
    public IReadOnlyList<ulong> ConnectedClientsIds
    {
        get
        {
            if (IsServer == false)
            {
                throw new NotServerException($"{nameof(m_ConnectedClientIds)} should only be accessed on server.");
            }
            return m_ConnectedClientIds;
        }
    }

    // KEEPSAKE FIX
    public NativeArray<ulong> ConnectedClientsIdsNativeArray
    {
        get
        {
            if (IsServer == false)
            {
                throw new NotServerException($"{nameof(m_ConnectedClientIdsNativeArray)} should only be accessed on server.");
            }
            return m_ConnectedClientIdsNativeArray;
        }
    }

    /// <summary>
    ///     Gets the local <see cref="NetworkClient" /> for this client.
    /// </summary>
    public NetworkClient LocalClient { get; internal set; }

    /// <summary>
    ///     Gets a dictionary of the clients that have been accepted by the transport but are still pending by the Netcode.
    ///     This is only populated on the server.
    /// </summary>
    public readonly Dictionary<ulong, PendingClient> PendingClients = new();

    /// <summary>
    ///     Gets Whether or not a server is running
    /// </summary>
    public bool IsServer { get; internal set; }

    /// <summary>
    ///     Gets Whether or not a client is running
    /// </summary>
    public bool IsClient { get; internal set; }

    /// <summary>
    ///     Gets if we are running as host
    /// </summary>
    public bool IsHost => IsServer && IsClient;

    /// <summary>
    ///     Gets Whether or not we are listening for connections
    /// </summary>
    public bool IsListening { get; internal set; }

    /// <summary>
    ///     Gets if we are connected as a client
    /// </summary>
    // KEEPSAKE FIX - made public
    public bool IsConnectedClient { get; set; }

    public bool ShutdownInProgress { get; private set; }

    /// <summary>
    ///     The callback to invoke once a client connects. This callback is only ran on the server and on the local client that
    ///     connects.
    /// </summary>
    public event Action<ulong> OnClientConnectedCallback;

    // KEEPSAKE FIX - made public
    public void InvokeOnClientConnectedCallback(ulong clientId)
    {
        OnClientConnectedCallback?.Invoke(clientId);
    }

    /// <summary>
    ///     The callback to invoke when a client disconnects. This callback is only ran on the server and on the local client
    ///     that disconnects.
    /// </summary>
    public event Action<ulong> OnClientDisconnectCallback;

    public event Action<ulong> OnClientDisconnectTransportCallback; // KEEPSAKE FIX - version with transportId

    /// <summary>
    ///     The callback to invoke once the server is ready
    /// </summary>
    public event Action OnServerStarted;

    // KEEPSAKE FIX
    /// <summary>
    ///     The callback to invoke once the client is ready
    /// </summary>
    public event Action OnClientStarted;

    /// <summary>
    ///     Delegate type called when connection has been approved. This only has to be set on the server.
    /// </summary>
    /// <param name="createPlayerObject">If true, a player object will be created. Otherwise the client will have no object.</param>
    /// <param name="playerPrefabHash">
    ///     The prefabHash to use for the client. If createPlayerObject is false, this is ignored.
    ///     If playerPrefabHash is null, the default player prefab is used.
    /// </param>
    /// <param name="approved">Whether or not the client was approved</param>
    /// <param name="position">The position to spawn the client at. If null, the prefab position is used.</param>
    /// <param name="rotation">The rotation to spawn the client with. If null, the prefab position is used.</param>
    public delegate void ConnectionApprovedDelegate(
        bool createPlayerObject,
        uint? playerPrefabHash,
        bool approved,
        Vector3? position,
        Quaternion? rotation);

    /// <summary>
    ///     The callback to invoke during connection approval
    /// </summary>
    public event Action<byte[], ulong, ConnectionApprovedDelegate> ConnectionApprovalCallback;

    internal void InvokeConnectionApproval(byte[] payload, ulong clientId, ConnectionApprovedDelegate action)
    {
        ConnectionApprovalCallback?.Invoke(payload, clientId, action);
    }

    /// <summary>
    ///     The current NetworkConfig
    /// </summary>
    [HideInInspector]
    public NetworkConfig NetworkConfig;

    /// <summary>
    ///     The current host name we are connected to, used to validate certificate
    /// </summary>
    public string ConnectedHostname { get; private set; }

    public INetworkMetrics NetworkMetrics { get; private set; }

    internal static event Action OnSingletonReady;

    #if UNITY_EDITOR
    private void OnValidate()
    {
        if (NetworkConfig == null)
        {
            return; // May occur when the component is added
        }

        if (GetComponentInChildren<NetworkObject>() != null)
        {
            if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
            {
                NetworkLog.LogWarning($"{nameof(NetworkManager)} cannot be a {nameof(NetworkObject)}.");
            }
        }

        var activeScene = UnityEngine.SceneManagement.SceneManager.GetActiveScene();

        // If the scene is not dirty or the asset database is currently updating then we can skip updating the NetworkPrefab information
        if (!activeScene.isDirty || EditorApplication.isUpdating)
        {
            return;
        }

        // During OnValidate we will always clear out NetworkPrefabOverrideLinks and rebuild it
        NetworkConfig.NetworkPrefabOverrideLinks.Clear();

        // Check network prefabs and assign to dictionary for quick look up
        for (var i = 0; i < NetworkConfig.NetworkPrefabs.Count; i++)
        {
            var networkPrefab = NetworkConfig.NetworkPrefabs[i];
            var networkPrefabGo = networkPrefab?.Prefab;
            if (networkPrefabGo != null)
            {
                var networkObject = networkPrefabGo.GetComponent<NetworkObject>();
                if (networkObject == null)
                {
                    if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
                    {
                        NetworkLog.LogError(
                            $"Cannot register {PrefabDebugHelper(networkPrefab)}, it does not have a {nameof(NetworkObject)} component at its root");
                    }
                }
                else
                {
                    {
                        var childNetworkObjects = new List<NetworkObject>();
                        networkPrefabGo.GetComponentsInChildren(true, childNetworkObjects);
                        if (childNetworkObjects.Count > 1) // total count = 1 root NetworkObject + n child NetworkObjects
                        {
                            if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
                            {
                                NetworkLog.LogWarning(
                                    $"{PrefabDebugHelper(networkPrefab)} has child {nameof(NetworkObject)}(s) but they will not be spawned across the network (unsupported {nameof(NetworkPrefab)} setup)");
                            }
                        }
                    }

                    // Default to the standard NetworkPrefab.Prefab's NetworkObject first
                    var globalObjectIdHash = networkObject.GlobalObjectIdHash;

                    // Now check to see if it has an override
                    switch (networkPrefab.Override)
                    {
                        case NetworkPrefabOverride.Prefab:
                        {
                            if (NetworkConfig.NetworkPrefabs[i].SourcePrefabToOverride == null
                                && NetworkConfig.NetworkPrefabs[i].Prefab != null)
                            {
                                if (networkPrefab.SourcePrefabToOverride == null)
                                {
                                    networkPrefab.SourcePrefabToOverride = networkPrefabGo;
                                }

                                globalObjectIdHash = networkPrefab.SourcePrefabToOverride.GetComponent<NetworkObject>().GlobalObjectIdHash;
                            }

                            break;
                        }
                        case NetworkPrefabOverride.Hash:
                            globalObjectIdHash = networkPrefab.SourceHashToOverride;
                            break;
                    }

                    // Add to the NetworkPrefabOverrideLinks or handle a new (blank) entries
                    if (!NetworkConfig.NetworkPrefabOverrideLinks.ContainsKey(globalObjectIdHash))
                    {
                        NetworkConfig.NetworkPrefabOverrideLinks.Add(globalObjectIdHash, networkPrefab);
                    }
                    else
                    {
                        // Duplicate entries can happen when adding a new entry into a list of existing entries
                        // Either this is user error or a new entry, either case we replace it with a new, blank, NetworkPrefab under this condition
                        NetworkConfig.NetworkPrefabs[i] = new NetworkPrefab();
                    }
                }
            }
        }
    }
    #endif

    private void Initialize(bool server)
    {
        // Don't allow the user to start a network session if the NetworkManager is
        // still parented under another GameObject
        if (NetworkManagerCheckForParent(true))
        {
            return;
        }

        if (NetworkLog.CurrentLogLevel <= LogLevel.Developer)
        {
            NetworkLog.LogInfo(nameof(Initialize));
        }

        this.RegisterNetworkUpdate(NetworkUpdateStage.EarlyUpdate);
        this.RegisterNetworkUpdate(NetworkUpdateStage.PostLateUpdate);

        MessagingSystem = new MessagingSystem(new NetworkManagerMessageSender(this), this);

        MessagingSystem.Hook(new NetworkManagerHooks(this));
        #if DEVELOPMENT_BUILD || UNITY_EDITOR
        MessagingSystem.Hook(new ProfilingHooks());
        #endif

        #if MULTIPLAYER_TOOLS
        MessagingSystem.Hook(new MetricHooks(this));
        #endif
        LocalClientId = ulong.MaxValue;

        PendingClients.Clear();
        m_ConnectedClients.Clear();
        m_ConnectedClientsList.Clear();
        m_ConnectedClientIds.Clear();

        // KEEPSAKE FIX
        ReallocateConnectedClientIdsNativeArray();

        LocalClient = null;
        NetworkObject.OrphanChildren.Clear();

        // Create spawn manager instance
        SpawnManager = new NetworkSpawnManager(this);

        CustomMessagingManager = new CustomMessagingManager(this);

        SceneManager = new NetworkSceneManager(this);

        BehaviourUpdater = new NetworkBehaviourUpdater();

        if (NetworkMetrics == null)
        {
            #if MULTIPLAYER_TOOLS
            NetworkMetrics = new NetworkMetrics();
            #else
                NetworkMetrics = new NullNetworkMetrics();
            #endif
        }

        #if MULTIPLAYER_TOOLS
        NetworkSolutionInterface.SetInterface(
            new NetworkSolutionInterfaceParameters
            {
                NetworkObjectProvider = new NetworkObjectProvider(this),
            });
        #endif

        if (NetworkConfig.NetworkTransport == null)
        {
            if (NetworkLog.CurrentLogLevel <= LogLevel.Error)
            {
                NetworkLog.LogError("No transport has been selected!");
            }

            return;
        }

        NetworkConfig.NetworkTransport.NetworkMetrics = NetworkMetrics;

        //This 'if' should never enter
        if (SnapshotSystem != null)
        {
            SnapshotSystem.Dispose();
            SnapshotSystem = null;
        }

        if (server)
        {
            NetworkTimeSystem = NetworkTimeSystem.ServerTimeSystem();
        }
        else
        {
            NetworkTimeSystem = new NetworkTimeSystem(1.0 / NetworkConfig.TickRate, k_DefaultBufferSizeSec, 0.2);
        }

        NetworkTickSystem = new NetworkTickSystem(NetworkConfig.TickRate, 0, 0);
        NetworkTickSystem.Tick += OnNetworkManagerTick;

        SnapshotSystem = new SnapshotSystem(this, NetworkConfig, NetworkTickSystem);

        this.RegisterNetworkUpdate(NetworkUpdateStage.PreUpdate);

        // This is used to remove entries not needed or invalid
        var removeEmptyPrefabs = new List<int>();

        // Always clear our prefab override links before building
        NetworkConfig.NetworkPrefabOverrideLinks.Clear();
        NetworkConfig.OverrideToNetworkPrefab.Clear();

        // Build the NetworkPrefabOverrideLinks dictionary
        for (var i = 0; i < NetworkConfig.NetworkPrefabs.Count; i++)
        {
            var sourcePrefabGlobalObjectIdHash = (uint)0;
            var targetPrefabGlobalObjectIdHash = (uint)0;
            var networkObject = (NetworkObject)null;
            // KEEPSAKE FIX - Addressable support
            if (NetworkConfig.NetworkPrefabs[i] == null
                || (NetworkConfig.NetworkPrefabs[i].Prefab == null
                    && NetworkConfig.NetworkPrefabs[i].AddressableKey == null
                    && NetworkConfig.NetworkPrefabs[i].Override == NetworkPrefabOverride.None))
            {
                if (NetworkLog.CurrentLogLevel <= LogLevel.Error)
                {
                    NetworkLog.LogWarning($"{nameof(NetworkPrefab)} cannot be null ({nameof(NetworkPrefab)} at index: {i})");
                }

                removeEmptyPrefabs.Add(i);
                continue;
            }
            if (NetworkConfig.NetworkPrefabs[i].Override == NetworkPrefabOverride.None)
            {
                var networkPrefab = NetworkConfig.NetworkPrefabs[i];

                // KEEPSAKE FIX - Addressables support
                if (networkPrefab.Prefab == null && !string.IsNullOrEmpty(networkPrefab.AddressableKey))
                {
                    sourcePrefabGlobalObjectIdHash = XXHash.Hash32(networkPrefab.AddressableKey);
                }
                else // END KEEPSAKE FIX
                {
                    networkObject = networkPrefab.Prefab.GetComponent<NetworkObject>();
                    if (networkObject == null)
                    {
                        if (NetworkLog.CurrentLogLevel <= LogLevel.Error)
                        {
                            NetworkLog.LogWarning(
                                $"{PrefabDebugHelper(networkPrefab)} is missing "
                                + $"a {nameof(NetworkObject)} component (entry will be ignored).");
                        }
                        removeEmptyPrefabs.Add(i);
                        continue;
                    }

                    // Otherwise get the GlobalObjectIdHash value
                    sourcePrefabGlobalObjectIdHash = networkObject.GlobalObjectIdHash;
                }
            }
            else // Validate Overrides
            {
                // Validate source prefab override values first
                switch (NetworkConfig.NetworkPrefabs[i].Override)
                {
                    case NetworkPrefabOverride.Hash:
                    {
                        if (NetworkConfig.NetworkPrefabs[i].SourceHashToOverride == 0)
                        {
                            if (NetworkLog.CurrentLogLevel <= LogLevel.Error)
                            {
                                NetworkLog.LogWarning(
                                    $"{nameof(NetworkPrefab)} {nameof(NetworkPrefab.SourceHashToOverride)} is zero "
                                    + "(entry will be ignored).");
                            }
                            removeEmptyPrefabs.Add(i);
                            continue;
                        }
                        sourcePrefabGlobalObjectIdHash = NetworkConfig.NetworkPrefabs[i].SourceHashToOverride;
                        break;
                    }
                    case NetworkPrefabOverride.Prefab:
                    {
                        if (NetworkConfig.NetworkPrefabs[i].SourcePrefabToOverride == null)
                        {
                            if (NetworkLog.CurrentLogLevel <= LogLevel.Error)
                            {
                                NetworkLog.LogWarning(
                                    $"{nameof(NetworkPrefab)} {nameof(NetworkPrefab.SourcePrefabToOverride)} is null (entry will be ignored).");
                            }
                            Debug.LogWarning(
                                $"{nameof(NetworkPrefab)} override entry {NetworkConfig.NetworkPrefabs[i].SourceHashToOverride} will be removed and ignored.");
                            removeEmptyPrefabs.Add(i);
                            continue;
                        }
                        networkObject = NetworkConfig.NetworkPrefabs[i].SourcePrefabToOverride.GetComponent<NetworkObject>();
                        if (networkObject == null)
                        {
                            if (NetworkLog.CurrentLogLevel <= LogLevel.Error)
                            {
                                NetworkLog.LogWarning(
                                    $"{nameof(NetworkPrefab)} ({NetworkConfig.NetworkPrefabs[i].SourcePrefabToOverride.name}) "
                                    + $"is missing a {nameof(NetworkObject)} component (entry will be ignored).");
                            }
                            Debug.LogWarning(
                                $"{nameof(NetworkPrefab)} override entry (\"{NetworkConfig.NetworkPrefabs[i].SourcePrefabToOverride.name}\") will be removed and ignored.");
                            removeEmptyPrefabs.Add(i);
                            continue;
                        }
                        sourcePrefabGlobalObjectIdHash = networkObject.GlobalObjectIdHash;
                        break;
                    }
                }

                // Validate target prefab override values next
                if (NetworkConfig.NetworkPrefabs[i].OverridingTargetPrefab == null)
                {
                    if (NetworkLog.CurrentLogLevel <= LogLevel.Error)
                    {
                        NetworkLog.LogWarning($"{nameof(NetworkPrefab)} {nameof(NetworkPrefab.OverridingTargetPrefab)} is null!");
                    }
                    removeEmptyPrefabs.Add(i);
                    switch (NetworkConfig.NetworkPrefabs[i].Override)
                    {
                        case NetworkPrefabOverride.Hash:
                        {
                            Debug.LogWarning(
                                $"{nameof(NetworkPrefab)} override entry {NetworkConfig.NetworkPrefabs[i].SourceHashToOverride} will be removed and ignored.");
                            break;
                        }
                        case NetworkPrefabOverride.Prefab:
                        {
                            Debug.LogWarning(
                                $"{nameof(NetworkPrefab)} override entry ({NetworkConfig.NetworkPrefabs[i].SourcePrefabToOverride.name}) will be removed and ignored.");
                            break;
                        }
                    }
                    continue;
                }
                targetPrefabGlobalObjectIdHash = NetworkConfig.NetworkPrefabs[i].OverridingTargetPrefab.GetComponent<NetworkObject>()
                                                              .GlobalObjectIdHash;
            }

            // Assign the appropriate GlobalObjectIdHash to the appropriate NetworkPrefab
            if (!NetworkConfig.NetworkPrefabOverrideLinks.ContainsKey(sourcePrefabGlobalObjectIdHash))
            {
                if (NetworkConfig.NetworkPrefabs[i].Override == NetworkPrefabOverride.None)
                {
                    NetworkConfig.NetworkPrefabOverrideLinks.Add(sourcePrefabGlobalObjectIdHash, NetworkConfig.NetworkPrefabs[i]);
                }
                else
                {
                    if (!NetworkConfig.OverrideToNetworkPrefab.ContainsKey(targetPrefabGlobalObjectIdHash))
                    {
                        switch (NetworkConfig.NetworkPrefabs[i].Override)
                        {
                            case NetworkPrefabOverride.Prefab:
                            {
                                NetworkConfig.NetworkPrefabOverrideLinks.Add(
                                    sourcePrefabGlobalObjectIdHash,
                                    NetworkConfig.NetworkPrefabs[i]);
                                NetworkConfig.OverrideToNetworkPrefab.Add(targetPrefabGlobalObjectIdHash, sourcePrefabGlobalObjectIdHash);
                            }
                                break;
                            case NetworkPrefabOverride.Hash:
                            {
                                NetworkConfig.NetworkPrefabOverrideLinks.Add(
                                    sourcePrefabGlobalObjectIdHash,
                                    NetworkConfig.NetworkPrefabs[i]);
                                NetworkConfig.OverrideToNetworkPrefab.Add(targetPrefabGlobalObjectIdHash, sourcePrefabGlobalObjectIdHash);
                            }
                                break;
                        }
                    }
                    else
                    {
                        // This can happen if a user tries to make several GlobalObjectIdHash values point to the same target
                        Debug.LogError(
                            $"{nameof(NetworkPrefab)} (\"{networkObject.name}\") has a duplicate {nameof(NetworkObject.GlobalObjectIdHash)} target entry value of: {targetPrefabGlobalObjectIdHash}! Removing entry from list!");
                        removeEmptyPrefabs.Add(i);
                    }
                }
            }
            else
            {
                // This should never happen, but in the case it somehow does log an error and remove the duplicate entry
                Debug.LogError(
                    $"{nameof(NetworkPrefab)} ({networkObject.name}) has a duplicate {nameof(NetworkObject.GlobalObjectIdHash)} source entry value of: {sourcePrefabGlobalObjectIdHash}! Removing entry from list!");
                removeEmptyPrefabs.Add(i);
            }
        }

        // If we have a player prefab, then we need to verify it is in the list of NetworkPrefabOverrideLinks for client side spawning.
        if (NetworkConfig.PlayerPrefab != null)
        {
            var playerPrefabNetworkObject = NetworkConfig.PlayerPrefab.GetComponent<NetworkObject>();
            if (playerPrefabNetworkObject != null)
            {
                //In the event there is no NetworkPrefab entry (i.e. no override for default player prefab)
                if (!NetworkConfig.NetworkPrefabOverrideLinks.ContainsKey(playerPrefabNetworkObject.GlobalObjectIdHash))
                {
                    //Then add a new entry for the player prefab
                    var playerNetworkPrefab = new NetworkPrefab();
                    playerNetworkPrefab.Prefab = NetworkConfig.PlayerPrefab;
                    NetworkConfig.NetworkPrefabs.Insert(0, playerNetworkPrefab);
                    NetworkConfig.NetworkPrefabOverrideLinks.Add(playerPrefabNetworkObject.GlobalObjectIdHash, playerNetworkPrefab);
                }
            }
            else
            {
                // Provide the name of the prefab with issues so the user can more easily find the prefab and fix it
                Debug.LogError(
                    $"{nameof(NetworkConfig.PlayerPrefab)} (\"{NetworkConfig.PlayerPrefab.name}\") has no NetworkObject assigned to it!.");
            }
        }

        // Clear out anything that is invalid or not used (for invalid entries we already logged warnings to the user earlier)
        // Iterate backwards so indices don't shift as we remove
        for (var i = removeEmptyPrefabs.Count - 1; i >= 0; i--)
        {
            NetworkConfig.NetworkPrefabs.RemoveAt(removeEmptyPrefabs[i]);
        }

        removeEmptyPrefabs.Clear();

        NetworkConfig.NetworkTransport.OnTransportEvent += HandleRawTransportPoll;

        NetworkConfig.NetworkTransport.Initialize();
    }

    /// <summary>
    ///     Starts a server
    /// </summary>
    public bool StartServer()
    {
        if (NetworkLog.CurrentLogLevel <= LogLevel.Developer)
        {
            NetworkLog.LogInfo("StartServer()");
        }

        if (IsServer || IsClient)
        {
            if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
            {
                NetworkLog.LogWarning("Cannot start server while an instance is already running");
            }

            return false;
        }

        if (NetworkConfig.ConnectionApproval)
        {
            if (ConnectionApprovalCallback == null)
            {
                if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
                {
                    NetworkLog.LogWarning("No ConnectionApproval callback defined. Connection approval will timeout");
                }
            }
        }

        Initialize(true);

        var result = NetworkConfig.NetworkTransport.StartServer();

        IsServer = true;
        IsClient = false;
        IsListening = true;

        SpawnManager.ServerSpawnSceneObjectsOnStartSweep();

        OnServerStarted?.Invoke();

        return result;
    }

    /// <summary>
    ///     Starts a client
    /// </summary>
    public bool StartClient()
    {
        // KEEPSAKE FIX - increase log verbosity while joining
        var origLogLevel = LogLevel;
        using var _ = Disposable.Create(() => LogLevel = origLogLevel);
        LogLevel = LogLevel.Developer;

        if (NetworkLog.CurrentLogLevel <= LogLevel.Developer)
        {
            NetworkLog.LogInfo(nameof(StartClient));
        }

        if (IsServer || IsClient)
        {
            if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
            {
                NetworkLog.LogWarning("Cannot start client while an instance is already running");
            }

            return false;
        }

        Initialize(false);
        MessagingSystem.ClientConnected(ServerClientId);

        var result = NetworkConfig.NetworkTransport.StartClient();

        IsServer = false;
        IsClient = true;
        IsListening = true;

        OnClientStarted?.Invoke();

        return result;
    }

    /// <summary>
    ///     Starts a Host
    /// </summary>
    public bool StartHost()
    {
        if (NetworkLog.CurrentLogLevel <= LogLevel.Developer)
        {
            NetworkLog.LogInfo(nameof(StartHost));
        }

        if (IsServer || IsClient)
        {
            if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
            {
                NetworkLog.LogWarning("Cannot start host while an instance is already running");
            }

            return false;
        }

        if (NetworkConfig.ConnectionApproval)
        {
            if (ConnectionApprovalCallback == null)
            {
                if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
                {
                    NetworkLog.LogWarning("No ConnectionApproval callback defined. Connection approval will timeout");
                }
            }
        }

        Initialize(true);

        var result = NetworkConfig.NetworkTransport.StartServer();
        MessagingSystem.ClientConnected(ServerClientId);
        LocalClientId = ServerClientId;
        NetworkMetrics.SetConnectionId(LocalClientId);

        IsServer = true;
        IsClient = true;
        IsListening = true;

        if (NetworkConfig.ConnectionApproval)
        {
            InvokeConnectionApproval(
                NetworkConfig.ConnectionData,
                ServerClientId,
                (createPlayerObject, playerPrefabHash, approved, position, rotation) =>
                {
                    // You cannot decline the local server. Force approved to true
                    if (!approved)
                    {
                        if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
                        {
                            NetworkLog.LogWarning("You cannot decline the host connection. The connection was automatically approved.");
                        }
                    }

                    HandleApproval(ServerClientId, createPlayerObject, playerPrefabHash, true, position, rotation);
                });
        }
        else
        {
            HandleApproval(ServerClientId, NetworkConfig.PlayerPrefab != null, null, true, null, null);
        }

        SpawnManager.ServerSpawnSceneObjectsOnStartSweep();

        OnServerStarted?.Invoke();

        return result;
    }

    public void SetSingleton()
    {
        Singleton = this;

        OnSingletonReady?.Invoke();
    }

    private void OnEnable()
    {
        if (RunInBackground)
        {
            Application.runInBackground = true;
        }

        if (Singleton == null)
        {
            SetSingleton();
        }

        if (!NetworkManagerCheckForParent())
        {
            // KEEPSAKE FIX - We want to control its lifetime, not have it in pesky DontDestroyOnLoad
            //DontDestroyOnLoad(gameObject);
        }
    }

    private void Awake()
    {
        UnityEngine.SceneManagement.SceneManager.sceneUnloaded += OnSceneUnloaded;
    }

    // Ensures that the NetworkManager is cleaned up before OnDestroy is run on NetworkObjects and NetworkBehaviours when unloading a scene with a NetworkManager
    private void OnSceneUnloaded(Scene scene)
    {
        // KEEPSAKE FIX - nullcheck
        if (this != null && gameObject != null && scene == gameObject.scene)
        {
            OnDestroy();
        }
    }

    // Ensures that the NetworkManager is cleaned up before OnDestroy is run on NetworkObjects and NetworkBehaviours when quitting the application.
    private void OnApplicationQuit()
    {
        OnDestroy();
    }

    // Note that this gets also called manually by OnSceneUnloaded and OnApplicationQuit
    private void OnDestroy()
    {
        ShutdownInternal();

        UnityEngine.SceneManagement.SceneManager.sceneUnloaded -= OnSceneUnloaded;

        if (Singleton == this)
        {
            Singleton = null;
        }
    }

    // KEEPSAKE FIX
    [RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.SubsystemRegistration)]
    private static void StaticInitialize()
    {
        Singleton = null;
    }
    // END KEEPSAKE FIX

    private void DisconnectRemoteClient(ulong clientId, bool graceful = true)
    {
        // KEEPSAKE FIX - Use our Try* instead to avoid exception when key is missing, is this being called multiple times?
        if (!TryClientIdToTransportId(clientId, out var transportId))
        {
            return;
        }
        NetworkConfig.NetworkTransport.DisconnectRemoteClient(transportId, graceful);
    }

    /// <summary>
    ///     Globally shuts down the library.
    ///     Disconnects clients if connected and stops server if running.
    /// </summary>
    /// <param name="discardMessageQueue">
    ///     If false, any messages that are currently in the incoming queue will be handled,
    ///     and any messages in the outgoing queue will be sent, before the shutdown is processed.
    ///     If true, NetworkManager will shut down immediately, and any unprocessed or unsent messages
    ///     will be discarded.
    /// </param>
    public void Shutdown(bool discardMessageQueue = false)
    {
        if (NetworkLog.CurrentLogLevel <= LogLevel.Developer)
        {
            NetworkLog.LogInfo(nameof(Shutdown));
        }

        ShutdownInProgress = true;
        m_StopProcessingMessages = discardMessageQueue;
    }

    internal void ShutdownInternal()
    {
        if (NetworkLog.CurrentLogLevel <= LogLevel.Developer)
        {
            NetworkLog.LogInfo(nameof(ShutdownInternal));
        }

        if (IsServer)
        {
            // make sure all messages are flushed before transport disconnect clients
            if (MessagingSystem != null)
            {
                MessagingSystem.ProcessSendQueues();
            }

            var disconnectedIds = new HashSet<ulong>();

            //Don't know if I have to disconnect the clients. I'm assuming the NetworkTransport does all the cleaning on shutdown. But this way the clients get a disconnect message from server (so long it does't get lost)

            // KEEPSAKE FIX - make copy of keys to avoid sometimes getting "collection was modified" exception when playmode in Editor is shutting down
            var keys = new List<ulong>(ConnectedClients.Keys);
            foreach (var key in keys)
            {
                if (!disconnectedIds.Contains(key))
                {
                    disconnectedIds.Add(key);

                    if (key == NetworkConfig.NetworkTransport.ServerClientId)
                    {
                        continue;
                    }

                    DisconnectRemoteClient(key);
                }
            }

            foreach (var pair in PendingClients)
            {
                if (!disconnectedIds.Contains(pair.Key))
                {
                    disconnectedIds.Add(pair.Key);
                    if (pair.Key == NetworkConfig.NetworkTransport.ServerClientId)
                    {
                        continue;
                    }

                    DisconnectRemoteClient(pair.Key);
                }
            }
        }

        // KEEPSAKE FIX - try/catch to not have failed d/c abort .e.g unregistering from play loop (network continues ticking in editor...)
        try
        {
            if (IsClient && IsConnectedClient)
            {
                // Client only, send disconnect to server
                NetworkConfig.NetworkTransport.DisconnectLocalClient();
            }
        }
        catch (Exception ex)
        {
            Debug.LogException(ex);
        }

        // KEEPSAKE FIX - clean up any initialized NetworkBehaviour on an inactive object, they won't get OnDestroy invoked
        foreach (var nb in m_InactiveInitializedBehaviours)
        {
            if (nb != null)
            {
                nb.DisposeVariables();
            }
        }
        m_InactiveInitializedBehaviours.Clear();

        IsConnectedClient = false;
        IsServer = false;
        IsClient = false;

        this.UnregisterAllNetworkUpdates();

        if (SnapshotSystem != null)
        {
            SnapshotSystem.Dispose();
            SnapshotSystem = null;
        }

        if (NetworkTickSystem != null)
        {
            NetworkTickSystem.Tick -= OnNetworkManagerTick;
            NetworkTickSystem = null;
        }

        if (m_InterestManager != null)
        {
            m_InterestManager = null;
        }

        if (MessagingSystem != null)
        {
            MessagingSystem.Dispose();
            MessagingSystem = null;
        }

        if (NetworkConfig?.NetworkTransport != null)
        {
            NetworkConfig.NetworkTransport.OnTransportEvent -= HandleRawTransportPoll;
        }

        if (SpawnManager != null)
        {
            SpawnManager.CleanupAllTriggers();
            SpawnManager.DespawnAndDestroyNetworkObjects();
            SpawnManager.ServerResetShudownStateForSceneObjects();

            SpawnManager = null;
        }

        if (SceneManager != null)
        {
            // Let the NetworkSceneManager clean up its two SceneEvenData instances
            SceneManager.Dispose();
            SceneManager = null;
        }

        if (CustomMessagingManager != null)
        {
            CustomMessagingManager = null;
        }

        if (BehaviourUpdater != null)
        {
            BehaviourUpdater = null;
        }

        // This is required for handling the potential scenario where multiple NetworkManager instances are created.
        // See MTT-860 for more information
        if (IsListening)
        {
            //The Transport is set during initialization, thus it is possible for the Transport to be null
            NetworkConfig?.NetworkTransport?.Shutdown();
        }

        // KEEPSAKE FIX
        if (m_ConnectedClientIdsNativeArray.IsCreated)
        {
            m_ConnectedClientIdsNativeArray.Dispose();
        }

        m_ClientIdToTransportIdMap.Clear();
        m_TransportIdToClientIdMap.Clear();

        IsListening = false;
        ShutdownInProgress = false;
        m_StopProcessingMessages = false;
    }

    // INetworkUpdateSystem
    public void NetworkUpdate(NetworkUpdateStage updateStage)
    {
        switch (updateStage)
        {
            case NetworkUpdateStage.EarlyUpdate:
                OnNetworkEarlyUpdate();
                break;
            case NetworkUpdateStage.PreUpdate:
                OnNetworkPreUpdate();
                break;
            case NetworkUpdateStage.PostLateUpdate:
                OnNetworkPostLateUpdate();
                break;
        }
    }

    private void OnNetworkEarlyUpdate()
    {
        if (!IsListening)
        {
            return;
        }

        #if DEVELOPMENT_BUILD || UNITY_EDITOR
        s_TransportPoll.Begin();
        #endif
        NetworkEvent networkEvent;
        do
        {
            networkEvent = NetworkConfig.NetworkTransport.PollEvent(out var clientId, out var payload, out var receiveTime);
            HandleRawTransportPoll(networkEvent, clientId, payload, receiveTime);
            // Only do another iteration if: there are no more messages AND (there is no limit to max events or we have processed less than the maximum)
        } while (IsListening && networkEvent != NetworkEvent.Nothing);

        MessagingSystem.ProcessIncomingMessageQueue();

        #if DEVELOPMENT_BUILD || UNITY_EDITOR
        s_TransportPoll.End();
        #endif
    }

    // TODO Once we have a way to subscribe to NetworkUpdateLoop with order we can move this out of NetworkManager but for now this needs to be here because we need strict ordering.
    private void OnNetworkPreUpdate()
    {
        if (IsServer == false && IsConnectedClient == false)
        {
            // As a client wait to run the time system until we are connected.
            return;
        }

        if (ShutdownInProgress && m_StopProcessingMessages)
        {
            return;
        }

        // Only update RTT here, server time is updated by time sync messages
        var reset = NetworkTimeSystem.Advance(Time.deltaTime);
        if (reset)
        {
            NetworkTickSystem.Reset(NetworkTimeSystem.LocalTime, NetworkTimeSystem.ServerTime);
        }
        NetworkTickSystem.UpdateTick(NetworkTimeSystem.LocalTime, NetworkTimeSystem.ServerTime);

        if (IsServer == false)
        {
            NetworkTimeSystem.Sync(
                NetworkTimeSystem.LastSyncedServerTimeSec + Time.deltaTime,
                NetworkConfig.NetworkTransport.GetCurrentRtt(ServerClientId) / 1000d);
        }
    }

    private void OnNetworkPostLateUpdate()
    {
        if (!ShutdownInProgress || !m_StopProcessingMessages)
        {
            MessagingSystem.ProcessSendQueues();
            NetworkMetrics.DispatchFrame();
        }
        SpawnManager.CleanupStaleTriggers();

        // KEEPSAKE FIX
        NetworkConfig.NetworkTransport.ProcessLatencyTrace();
        // END KEEPSAKE FIX

        if (ShutdownInProgress)
        {
            ShutdownInternal();
        }
    }

    /// <summary>
    ///     This function runs once whenever the local tick is incremented and is responsible for the following (in order):
    ///     - collect commands/inputs and send them to the server (TBD)
    ///     - call NetworkFixedUpdate on all NetworkBehaviours in prediction/client authority mode
    ///     - create a snapshot from resulting state
    /// </summary>
    private void OnNetworkManagerTick()
    {
        // Do NetworkVariable updates
        BehaviourUpdater.NetworkBehaviourUpdate(this);

        var timeSyncFrequencyTicks = (int)(k_TimeSyncFrequency * NetworkConfig.TickRate);
        if (IsServer && NetworkTickSystem.ServerTime.Tick % timeSyncFrequencyTicks == 0)
        {
            SyncTime();
        }
    }

    private void SendConnectionRequest()
    {
        var message = new ConnectionRequestMessage
        {
            ConfigHash = NetworkConfig.GetConfig(),
            ShouldSendConnectionData = NetworkConfig.ConnectionApproval,
            ConnectionData = NetworkConfig.ConnectionData,
        };
        SendMessage(ref message, NetworkDelivery.ReliableSequenced, ServerClientId);
    }

    private IEnumerator ApprovalTimeout(ulong clientId)
    {
        var timeStarted = LocalTime;

        //We yield every frame incase a pending client disconnects and someone else gets its connection id
        while ((LocalTime - timeStarted).Time < NetworkConfig.ClientConnectionBufferTimeout && PendingClients.ContainsKey(clientId))
        {
            yield return null;
        }

        if (PendingClients.ContainsKey(clientId) && !ConnectedClients.ContainsKey(clientId))
        {
            // Timeout
            if (NetworkLog.CurrentLogLevel <= LogLevel.Developer)
            {
                NetworkLog.LogInfo($"Client {clientId} Handshake Timed Out");
            }

            DisconnectClient(clientId);
        }
    }

    private ulong TransportIdToClientId(ulong transportId)
    {
        return transportId == m_ServerTransportId ? ServerClientId : m_TransportIdToClientIdMap[transportId];
    }

    private ulong ClientIdToTransportId(ulong clientId)
    {
        return clientId == ServerClientId ? m_ServerTransportId : m_ClientIdToTransportIdMap[clientId];
    }

    // KEEPSAKE FIX - added Try-variant
    private bool TryClientIdToTransportId(ulong clientId, out ulong transportId)
    {
        if (clientId == ServerClientId)
        {
            transportId = m_ServerTransportId;
            return true;
        }

        return m_ClientIdToTransportIdMap.TryGetValue(clientId, out transportId);
    }

    private void HandleRawTransportPoll(NetworkEvent networkEvent, ulong clientId, ArraySegment<byte> payload, float receiveTime)
    {
        var transportId = clientId;
        switch (networkEvent)
        {
            case NetworkEvent.Connect:
                #if DEVELOPMENT_BUILD || UNITY_EDITOR
                s_TransportConnect.Begin();
                #endif

                clientId = m_NextClientId++;
                m_ClientIdToTransportIdMap[clientId] = transportId;
                m_TransportIdToClientIdMap[transportId] = clientId;

                MessagingSystem.ClientConnected(clientId);
                if (IsServer)
                {
                    if (NetworkLog.CurrentLogLevel <= LogLevel.Developer)
                    {
                        NetworkLog.LogInfo("Client Connected");
                    }

                    PendingClients.Add(
                        clientId,
                        new PendingClient
                        {
                            ClientId = clientId,
                            ConnectionState = PendingClient.State.PendingConnection,
                        });

                    StartCoroutine(ApprovalTimeout(clientId));
                }
                else
                {
                    if (NetworkLog.CurrentLogLevel <= LogLevel.Developer)
                    {
                        NetworkLog.LogInfo("Connected");
                    }

                    SendConnectionRequest();
                    StartCoroutine(ApprovalTimeout(clientId));
                }

                #if DEVELOPMENT_BUILD || UNITY_EDITOR
                s_TransportConnect.End();
                #endif
                break;
            case NetworkEvent.Data:
            {
                if (NetworkLog.CurrentLogLevel <= LogLevel.Developer)
                {
                    NetworkLog.LogInfo($"Incoming Data From {clientId}: {payload.Count} bytes");
                }

                clientId = TransportIdToClientId(clientId);

                HandleIncomingData(clientId, payload, receiveTime);
                break;
            }
            case NetworkEvent.Disconnect:
                #if DEVELOPMENT_BUILD || UNITY_EDITOR
                s_TransportDisconnect.Begin();
                #endif
                clientId = TransportIdToClientId(clientId);

                OnClientDisconnectCallback?.Invoke(clientId);
                OnClientDisconnectTransportCallback?.Invoke(transportId);

                m_TransportIdToClientIdMap.Remove(transportId);
                m_ClientIdToTransportIdMap.Remove(clientId);

                if (NetworkLog.CurrentLogLevel <= LogLevel.Developer)
                {
                    NetworkLog.LogInfo($"Disconnect Event From {clientId}");
                }

                if (IsServer)
                {
                    OnClientDisconnectFromServer(clientId);
                }
                else
                {
                    Shutdown();
                }
                #if DEVELOPMENT_BUILD || UNITY_EDITOR
                s_TransportDisconnect.End();
                #endif
                break;
        }
    }

    internal unsafe int SendMessage<TMessageType, TClientIdListType>(
        ref TMessageType message,
        NetworkDelivery delivery,
        in TClientIdListType clientIds) where TMessageType : INetworkMessage where TClientIdListType : IReadOnlyList<ulong>
    {
        // Prevent server sending to itself
        if (IsServer)
        {
            var nonServerIds = stackalloc ulong[clientIds.Count];
            var newIdx = 0;
            for (var idx = 0; idx < clientIds.Count; ++idx)
            {
                if (clientIds[idx] == ServerClientId)
                {
                    continue;
                }

                nonServerIds[newIdx++] = clientIds[idx];
            }

            if (newIdx == 0)
            {
                return 0;
            }
            return MessagingSystem.SendMessage(ref message, delivery, nonServerIds, newIdx);
        }
        return MessagingSystem.SendMessage(ref message, delivery, clientIds);
    }

    internal unsafe int SendMessage<T>(ref T message, NetworkDelivery delivery, ulong* clientIds, int numClientIds)
        where T : INetworkMessage
    {
        // Prevent server sending to itself
        if (IsServer)
        {
            var nonServerIds = stackalloc ulong[numClientIds];
            var newIdx = 0;
            for (var idx = 0; idx < numClientIds; ++idx)
            {
                if (clientIds[idx] == ServerClientId)
                {
                    continue;
                }

                nonServerIds[newIdx++] = clientIds[idx];
            }

            if (newIdx == 0)
            {
                return 0;
            }
            return MessagingSystem.SendMessage(ref message, delivery, nonServerIds, newIdx);
        }

        return MessagingSystem.SendMessage(ref message, delivery, clientIds, numClientIds);
    }

    // KEEPSAKE FIX - make public
    public unsafe int SendMessage<T>(ref T message, NetworkDelivery delivery, in NativeArray<ulong> clientIds) where T : INetworkMessage
    {
        return SendMessage(ref message, delivery, (ulong*)clientIds.GetUnsafePtr(), clientIds.Length);
    }

    // KEEPSAKE FIX - make public
    public int SendMessage<T>(ref T message, NetworkDelivery delivery, ulong clientId) where T : INetworkMessage
    {
        // Prevent server sending to itself
        if (IsServer && clientId == ServerClientId)
        {
            return 0;
        }
        return MessagingSystem.SendMessage(ref message, delivery, clientId);
    }

    internal void HandleIncomingData(ulong clientId, ArraySegment<byte> payload, float receiveTime)
    {
        #if DEVELOPMENT_BUILD || UNITY_EDITOR
        s_HandleIncomingData.Begin();
        #endif

        MessagingSystem.HandleIncomingData(clientId, payload, receiveTime);

        #if DEVELOPMENT_BUILD || UNITY_EDITOR
        s_HandleIncomingData.End();
        #endif
    }

    /// <summary>
    ///     Disconnects the remote client.
    /// </summary>
    /// <param name="clientId">The ClientId to disconnect</param>
    /// <param name="graceful">KEEPSAKE FIX</param>
    public void DisconnectClient(ulong clientId, bool graceful = true)
    {
        if (!IsServer)
        {
            throw new NotServerException("Only server can disconnect remote clients. Use StopClient instead.");
        }

        OnClientDisconnectFromServer(clientId);
        DisconnectRemoteClient(clientId, graceful);
    }

    private void OnClientDisconnectFromServer(ulong clientId)
    {
        Debug.Log($"OnClientDisconnectFromServer {clientId}");
        PendingClients.Remove(clientId);

        if (ConnectedClients.TryGetValue(clientId, out var networkClient))
        {
            if (IsServer)
            {
                var playerObject = networkClient.PlayerObject;
                if (playerObject != null)
                {
                    // KEEPSAKE FIX
                    InterestManager.OnClientDisconnected(playerObject);

                    // As long as we can destroy the PlayerObject with the owner
                    // KEEPSAKE FIX - rename to not negate
                    if (playerObject.DestroyWithOwner)
                    {
                        if (PrefabHandler.ContainsHandler(ConnectedClients[clientId].PlayerObject.GlobalObjectIdHash))
                        {
                            PrefabHandler.HandleNetworkPrefabDestroy(ConnectedClients[clientId].PlayerObject);
                        }
                        else
                        {
                            Destroy(playerObject.gameObject);
                        }
                    }
                    else // Otherwise, just remove the ownership
                    {
                        playerObject.RemoveOwnership();
                    }
                }

                for (var i = networkClient.OwnedObjects.Count - 1; i >= 0; i--)
                {
                    var ownedObject = networkClient.OwnedObjects[i];
                    if (ownedObject != null)
                    {
                        // KEEPSAKE FIX - rename to not negate
                        if (ownedObject.DestroyWithOwner)
                        {
                            if (PrefabHandler.ContainsHandler(ConnectedClients[clientId].OwnedObjects[i].GlobalObjectIdHash))
                            {
                                PrefabHandler.HandleNetworkPrefabDestroy(ConnectedClients[clientId].OwnedObjects[i]);
                            }
                            else
                            {
                                Destroy(ownedObject.gameObject);
                            }
                        }
                        else
                        {
                            ownedObject.RemoveOwnership();
                        }
                    }
                }

                // TODO: Could(should?) be replaced with more memory per client, by storing the visibility

                // KEEPSAKE FIX - find objects in Attached instead of Spawned collection
                foreach (var sobj in SpawnManager.AttachedObjectsList)
                {
                    sobj.Observers.Remove(clientId);
                }
            }

            for (var i = 0; i < ConnectedClientsList.Count; i++)
            {
                if (ConnectedClientsList[i].ClientId == clientId)
                {
                    m_ConnectedClientsList.RemoveAt(i);
                    break;
                }
            }

            for (var i = 0; i < ConnectedClientsIds.Count; i++)
            {
                if (ConnectedClientsIds[i] == clientId)
                {
                    m_ConnectedClientIds.RemoveAt(i);
                    break;
                }
            }

            ReallocateConnectedClientIdsNativeArray();

            m_ConnectedClients.Remove(clientId);
        }
        Debug.Log($"Telling MessagingSystem about disconnected client {clientId}");
        MessagingSystem.ClientDisconnected(clientId);
    }

    // KEEPSAKE FIX
    private void ReallocateConnectedClientIdsNativeArray()
    {
        if (m_ConnectedClientIdsNativeArray.IsCreated)
        {
            m_ConnectedClientIdsNativeArray.Dispose();
        }

        m_ConnectedClientIdsNativeArray = m_ConnectedClientIds.ToNativeArray(Allocator.Persistent);
    }

    private void SyncTime()
    {
        #if DEVELOPMENT_BUILD || UNITY_EDITOR
        s_SyncTime.Begin();
        #endif
        if (NetworkLog.CurrentLogLevel <= LogLevel.Developer)
        {
            NetworkLog.LogInfo("Syncing Time To Clients");
        }

        var message = new TimeSyncMessage
        {
            Tick = NetworkTickSystem.ServerTime.Tick,
        };
        SendMessage(ref message, NetworkDelivery.Unreliable, ConnectedClientsIds);
        #if DEVELOPMENT_BUILD || UNITY_EDITOR
        s_SyncTime.End();
        #endif
    }

    /// <summary>
    ///     Server Side: Handles the approval of a client
    /// </summary>
    /// <param name="ownerClientId">client being approved</param>
    /// <param name="createPlayerObject">whether we want to create a player or not</param>
    /// <param name="playerPrefabHash">the GlobalObjectIdHash value for the Network Prefab to create as the player</param>
    /// <param name="approved">Is the player approved or not?</param>
    /// <param name="position">Used when createPlayerObject is true, position of the player when spawned </param>
    /// <param name="rotation">Used when createPlayerObject is true, rotation of the player when spawned</param>
    internal void HandleApproval(
        ulong ownerClientId,
        bool createPlayerObject,
        uint? playerPrefabHash,
        bool approved,
        Vector3? position,
        Quaternion? rotation)
    {
        if (approved)
        {
            HandleApprovalAsync(ownerClientId, createPlayerObject, playerPrefabHash, position, rotation).Forget();
        }
        else
        {
            PendingClients.Remove(ownerClientId);
            DisconnectRemoteClient(ownerClientId);
        }
    }

    private async UniTask HandleApprovalAsync(
        ulong ownerClientId,
        bool createPlayerObject,
        uint? playerPrefabHash,
        Vector3? position,
        Quaternion? rotation)
    {
        // Inform new client it got approved
        PendingClients.Remove(ownerClientId);

        var client = new NetworkClient { ClientId = ownerClientId };
        m_ConnectedClients.Add(ownerClientId, client);
        m_ConnectedClientsList.Add(client);
        m_ConnectedClientIds.Add(client.ClientId);
        ReallocateConnectedClientIdsNativeArray();

        if (createPlayerObject)
        {
            await SpawnPlayerObjectAsync(playerPrefabHash, ownerClientId, position, rotation);
        }

        // Server doesn't send itself the connection approved message
        if (ownerClientId != ServerClientId)
        {
            var message = new ConnectionApprovedMessage
            {
                OwnerClientId = ownerClientId,
                NetworkTick = LocalTime.Tick,
            };

            // KEEPSAKE FIX - use our more granular settings
            if (NetworkConfig.SyncAllNetworkObjectsInConnectionApprovedMessage
                || NetworkConfig.SyncPlayerObjectsInConnectionApprovedMessage)
            {
                // KEEPSAKE NOTE - sync Spawned, not Attached
                if (SpawnManager.SpawnedObjectsList.Count != 0)
                {
                    message.SpawnedObjectsList = SpawnManager.SpawnedObjectsList;
                }

                message.SyncOnlyPlayerObjects = NetworkConfig.SyncAllNetworkObjectsInConnectionApprovedMessage == false
                    && NetworkConfig.SyncPlayerObjectsInConnectionApprovedMessage;
            }

            SendMessage(ref message, NetworkDelivery.ReliableFragmentedSequenced, ownerClientId);

            // If scene management is enabled, then let NetworkSceneManager handle the initial scene and NetworkObject synchronization
            if (!NetworkConfig.EnableSceneManagement)
            {
                InvokeOnClientConnectedCallback(ownerClientId);
            }
            else
            {
                SceneManager.SynchronizeNetworkObjects(ownerClientId);
            }
        }
        else // Server just adds itself as an observer to all spawned NetworkObjects
        {
            LocalClient = client;
            SpawnManager.UpdateObservedNetworkObjects(ownerClientId);
            InvokeOnClientConnectedCallback(ownerClientId);
        }

        if (!createPlayerObject || (playerPrefabHash == null && NetworkConfig.PlayerPrefab == null))
        {
            return;
        }

        // Separating this into a contained function call for potential further future separation of when this notification is sent.
        ApprovedPlayerSpawn(ownerClientId, playerPrefabHash ?? NetworkConfig.PlayerPrefab.GetComponent<NetworkObject>().GlobalObjectIdHash);
    }

    // KEEPSAKE FIX - broken out to support async local object creation
    private async UniTask SpawnPlayerObjectAsync(uint? playerPrefabHash, ulong ownerClientId, Vector3? position, Quaternion? rotation)
    {
        var networkObject = await SpawnManager.CreateLocalNetworkObjectAsync(
            false,
            playerPrefabHash ?? NetworkConfig.PlayerPrefab.GetComponent<NetworkObject>().GlobalObjectIdHash,
            ownerClientId,
            null,
            null,
            false,
            position,
            rotation);
        SpawnManager.SpawnNetworkObjectLocally(networkObject, SpawnManager.GetNetworkObjectId(), false, true, ownerClientId, false);

        ConnectedClients[ownerClientId].PlayerObject = networkObject;
    }

    /// <summary>
    ///     Spawns the newly approved player
    /// </summary>
    /// <param name="clientId">new player client identifier</param>
    /// <param name="playerPrefabHash">the prefab GlobalObjectIdHash value for this player</param>
    internal void ApprovedPlayerSpawn(ulong clientId, uint playerPrefabHash)
    {
        foreach (var clientPair in ConnectedClients)
        {
            if (clientPair.Key == clientId
                || clientPair.Key == ServerClientId
                || // Server already spawned it
                ConnectedClients[clientId].PlayerObject == null
                || !ConnectedClients[clientId].PlayerObject.Observers.Contains(clientPair.Key))
            {
                continue; //The new client.
            }

            var message = new CreateObjectMessage
            {
                ObjectInfo = ConnectedClients[clientId].PlayerObject.GetMessageSceneObject(clientPair.Key, false),
            };
            message.ObjectInfo.Header.Hash = playerPrefabHash;
            message.ObjectInfo.Header.IsSceneObject = false;
            message.ObjectInfo.Header.HasParent = false;
            message.ObjectInfo.Header.WaitForParentIfMissing = false; // KEEPSAKE FIX
            message.ObjectInfo.Header.IsPlayerObject = true;
            message.ObjectInfo.Header.OwnerClientId = clientId;
            var size = SendMessage(ref message, NetworkDelivery.ReliableFragmentedSequenced, clientPair.Key);
            NetworkMetrics.TrackObjectSpawnSent(clientPair.Key, ConnectedClients[clientId].PlayerObject, size);
        }
    }

    /// <summary>
    ///     Handle runtime detection for parenting the NetworkManager's GameObject under another GameObject
    /// </summary>
    private void OnTransformParentChanged()
    {
        NetworkManagerCheckForParent();
    }

    // KEEPSAKE FIX - extra tracking for inactive but initialized network behaviours since we need to make sure they're cleaned up
    public void StartTrackingInactiveInitializedBehaviour(NetworkBehaviour networkBehaviour)
    {
        m_InactiveInitializedBehaviours.Add(networkBehaviour);
    }

    public void StopTrackingInactiveInitializedBehaviour(NetworkBehaviour networkBehaviour)
    {
        m_InactiveInitializedBehaviours.Remove(networkBehaviour);
    }
    // END KEEPSAKE FIX

    /// <summary>
    ///     Determines if the NetworkManager's GameObject is parented under another GameObject and
    ///     notifies the user that this is not allowed for the NetworkManager.
    /// </summary>
    internal bool NetworkManagerCheckForParent(bool ignoreNetworkManagerCache = false)
    {
        #if UNITY_EDITOR
        var isParented = NetworkManagerHelper.NotifyUserOfNestedNetworkManager(this, ignoreNetworkManagerCache);
        #else
            var isParented = transform.root != transform;
            if (isParented)
            {
                throw new Exception(GenerateNestedNetworkManagerMessage(transform));
            }
        #endif
        return isParented;
    }

    internal static string GenerateNestedNetworkManagerMessage(Transform transform)
    {
        return $"{transform.name} is nested under {transform.root.name}. NetworkManager cannot be nested.\n";
    }

    #if UNITY_EDITOR
    internal static INetworkManagerHelper NetworkManagerHelper;
    /// <summary>
    ///     Interface for NetworkManagerHelper
    /// </summary>
    internal interface INetworkManagerHelper
    {
        bool NotifyUserOfNestedNetworkManager(
            NetworkManager networkManager,
            bool ignoreNetworkManagerCache = false,
            bool editorTest = false);
    }
    #endif
}

}
