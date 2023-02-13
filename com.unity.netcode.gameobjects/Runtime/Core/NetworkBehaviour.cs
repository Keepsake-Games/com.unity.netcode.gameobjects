using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using UniRx;
using UniRx.Observers;
using Unity.Collections;
using UnityEngine;
using UnityEngine.Assertions;

namespace Unity.Netcode
{

/// <summary>
///     The base class to override to write network code. Inherits MonoBehaviour
/// </summary>
public abstract class NetworkBehaviour : MonoBehaviour
{
    private const int k_RpcMessageDefaultSize = 1024;      // 1k
    private const int k_RpcMessageMaximumSize = 1024 * 64; // 64k

    private static readonly Dictionary<Type, FieldInfo[]> s_FieldTypes = new();

    // KEEPSAKE FIX - made protected to access for our Blackboard net syncer
    protected readonly List<HashSet<int>> m_DeliveryMappedNetworkVariableIndices = new();

    // KEEPSAKE FIX - made protected to access for our Blackboard net syncer
    protected readonly List<NetworkDelivery> m_DeliveryTypesForNetworkVariableGroups = new();

    // KEEPSAKE FIX - made protected internal to access for our Blackboard net syncer
    protected internal readonly List<NetworkVariableBase> NetworkVariableFields = new();

    // KEEPSAKE FIX - observable IsDirty
    private readonly List<IDisposable> NetworkVariableDirtySubscriptions = new();

    internal readonly List<int>    NetworkVariableIndexesToReset    = new();
    internal readonly HashSet<int> NetworkVariableIndexesToResetSet = new();

    #pragma warning disable IDE1006 // disable naming rule violation check
    [NonSerialized]
    // RuntimeAccessModifiersILPP will make this `protected`
    internal __RpcExecStage __rpc_exec_stage = __RpcExecStage.None;
    #pragma warning restore IDE1006 // restore naming rule violation check

    private NetworkObject m_NetworkObject;

    private bool m_VarInit;

    private bool m_VarInitWhileInactive;

    /// <summary>
    ///     Internally caches the Id of this behaviour in a NetworkObject. Makes look-up faster
    /// </summary>
    internal ushort NetworkBehaviourIdCache = 0;

    /// <summary>
    ///     Gets the NetworkManager that owns this NetworkBehaviour instance
    ///     See note around `NetworkObject` for how there is a chicken / egg problem when we are not initialized
    /// </summary>
    public NetworkManager NetworkManager => NetworkObject.NetworkManager;

    /// <summary>
    ///     Gets if the object is the the personal clients player object
    /// </summary>
    public bool IsLocalPlayer => NetworkObject.IsLocalPlayer;

    /// <summary>
    ///     Gets if the object is owned by the local player or if the object is the local player object
    /// </summary>
    public bool IsOwner => NetworkObject.IsOwner;

    /// <summary>
    ///     Gets if we are executing as server
    /// </summary>
    protected bool IsServer => IsRunning && NetworkManager.IsServer;

    /// <summary>
    ///     Gets if we are executing as client
    /// </summary>
    protected bool IsClient => IsRunning && NetworkManager.IsClient;

    /// <summary>
    ///     Gets if we are executing as Host, I.E Server and Client
    /// </summary>
    protected bool IsHost => IsRunning && NetworkManager.IsHost;

    private bool IsRunning => NetworkManager && NetworkManager.IsListening;

    // KEEPSAKE FIX - observable IsDirty
    private int m_DirtyVariableCount;

    /// <summary>
    ///     Gets Whether or not the object has a owner
    /// </summary>
    public bool IsOwnedByServer => NetworkObject.IsOwnedByServer;

    /// <summary>
    ///     Used to determine if it is safe to access NetworkObject and NetworkManager from within a NetworkBehaviour component
    ///     Primarily useful when checking NetworkObject/NetworkManager properties within FixedUpate
    /// </summary>
    // KEEPSAKE FIX - check IsAttached and not IsSpawned since this "IsSpawned" prop is more like "is network ready"
    public bool IsSpawned => HasNetworkObject ? NetworkObject.IsAttached : false;

    // KEEPSAKE FIX - we want to differentiate between sets from the layer itself or sets by the user
    public bool IsInternalVariableWrite { get; internal set; }

    /// <summary>
    ///     Gets the NetworkObject that owns this NetworkBehaviour instance
    ///     TODO: this needs an overhaul.  It's expensive, it's ja little naive in how it looks for networkObject in
    ///     its parent and worst, it creates a puzzle if you are a NetworkBehaviour wanting to see if you're live or not
    ///     (e.g. editor code).  All you want to do is find out if NetworkManager is null, but to do that you
    ///     need NetworkObject, but if you try and grab NetworkObject and NetworkManager isn't up you'll get
    ///     the warning below.  This is why IsBehaviourEditable had to be created.  Matt was going to re-do
    ///     how NetworkObject works but it was close to the release and too risky to change
    /// </summary>
    public NetworkObject NetworkObject
    {
        get
        {
            if (m_NetworkObject == null)
            {
                // KEEPSAKE FIX - include inactive parents when looking, as we want to deactivate some instances on Awake and still allow network to init properly
                m_NetworkObject = GetComponentInParent<NetworkObject>(true);
            }

            // KEEPSAKE FIX - let it NRE as these checks are costly
            /*if (m_NetworkObject == null || NetworkManager.Singleton == null || (NetworkManager.Singleton != null && !NetworkManager.Singleton.ShutdownInProgress))
            {
                if (NetworkLog.CurrentLogLevel < LogLevel.Normal)
                {
                    NetworkLog.LogWarning($"Could not get {nameof(NetworkObject)} for the {nameof(NetworkBehaviour)}. Are you missing a {nameof(NetworkObject)} component?");
                }
            }*/

            return m_NetworkObject;
        }
    }

    /// <summary>
    ///     Gets whether or not this NetworkBehaviour instance has a NetworkObject owner.
    /// </summary>
    public bool HasNetworkObject => NetworkObject != null;

    /// <summary>
    ///     Gets the NetworkId of the NetworkObject that owns this NetworkBehaviour
    /// </summary>
    public ulong NetworkObjectId => NetworkObject.NetworkObjectId;

    /// <summary>
    ///     Gets NetworkId for this NetworkBehaviour from the owner NetworkObject
    /// </summary>
    public ushort NetworkBehaviourId => NetworkObject.GetNetworkBehaviourOrderIndex(this);

    /// <summary>
    ///     Gets the ClientId that owns the NetworkObject
    /// </summary>
    public ulong OwnerClientId => NetworkObject.OwnerClientId;

    // KEEPSAKE FIX
    private void OnEnable()
    {
        if (m_VarInit && m_VarInitWhileInactive && NetworkManager != null && !NetworkManager.ShutdownInProgress)
        {
            NetworkManager.StopTrackingInactiveInitializedBehaviour(this);
        }
    }

    private void OnDisable()
    {
        if (m_VarInit && NetworkManager != null && !NetworkManager.ShutdownInProgress)
        {
            NetworkManager.StartTrackingInactiveInitializedBehaviour(this);
        }
    }

    // KEEPSAKE FIX - eagerly lookup our NetworkObject
    private void Start()
    {
        var _ = NetworkObject;
    }

    public virtual void OnDestroy()
    {
        // this seems odd to do here, but in fact especially in tests we can find ourselves
        //  here without having called InitializedVariables, which causes problems if any
        //  of those variables use native containers (e.g. NetworkList) as they won't be
        //  registered here and therefore won't be cleaned up.
        //
        // we should study to understand the initialization patterns
        if (!m_VarInit)
        {
            InitializeVariables();
        }

        // KEEPSAKE FIX - moved to standalone DisposeVariables()
        DisposeVariables();
    }

    #pragma warning disable IDE1006 // disable naming rule violation check
    // NetworkBehaviourILPP will override this in derived classes to return the name of the concrete type
    internal virtual string __getTypeName()
    {
        return nameof(NetworkBehaviour);
    }
    #pragma warning restore IDE1006 // restore naming rule violation check

    #pragma warning disable IDE1006 // disable naming rule violation check
    // RuntimeAccessModifiersILPP will make this `protected`
    internal FastBufferWriter __beginSendServerRpc(uint rpcMethodId, ServerRpcParams serverRpcParams, RpcDelivery rpcDelivery)
        #pragma warning restore IDE1006 // restore naming rule violation check
    {
        return new FastBufferWriter(k_RpcMessageDefaultSize, Allocator.Temp, k_RpcMessageMaximumSize);
    }

    #pragma warning disable IDE1006 // disable naming rule violation check
    // RuntimeAccessModifiersILPP will make this `protected`
    internal void __endSendServerRpc(ref FastBufferWriter bufferWriter, uint rpcMethodId, ServerRpcParams serverRpcParams, RpcDelivery rpcDelivery, /* KEEPSAKE FIX */ bool withLocalPrediction)
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

        var rpcWriteSize = 0;

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
            NetworkManager.NetworkMetrics.TrackRpcSent(NetworkManager.ServerClientId, NetworkObject, rpcMethodName, __getTypeName(), rpcWriteSize);
        }
        #endif
    }

    #pragma warning disable IDE1006 // disable naming rule violation check
    // RuntimeAccessModifiersILPP will make this `protected`
    internal FastBufferWriter __beginSendClientRpc(uint rpcMethodId, ClientRpcParams clientRpcParams, RpcDelivery rpcDelivery)
        #pragma warning restore IDE1006 // restore naming rule violation check
    {
        return new FastBufferWriter(k_RpcMessageDefaultSize, Allocator.Temp, k_RpcMessageMaximumSize);
    }

    #pragma warning disable IDE1006 // disable naming rule violation check
    // RuntimeAccessModifiersILPP will make this `protected`
    internal void __endSendClientRpc(ref FastBufferWriter bufferWriter, uint rpcMethodId, ClientRpcParams clientRpcParams, RpcDelivery rpcDelivery)
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

        var rpcWriteSize = 0;

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

            rpcWriteSize = NetworkManager.SendMessage(ref clientRpcMessage, networkDelivery, clientRpcParams.Send.TargetClientIdsNativeArray.Value);
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

    internal bool IsBehaviourEditable()
    {
        // Only server can MODIFY. So allow modification if network is either not running or we are server
        return !m_NetworkObject || m_NetworkObject.NetworkManager == null || !m_NetworkObject.NetworkManager.IsListening || m_NetworkObject.NetworkManager.IsServer;
    }

    /// <summary>
    ///     Returns a the NetworkBehaviour with a given BehaviourId for the current NetworkObject
    /// </summary>
    /// <param name="behaviourId">The behaviourId to return</param>
    /// <returns>Returns NetworkBehaviour with given behaviourId</returns>
    protected NetworkBehaviour GetNetworkBehaviour(ushort behaviourId)
    {
        return NetworkObject.GetNetworkBehaviourAtOrderIndex(behaviourId);
    }

    // KEEPSAKE FIX - special callback so that LegacyNetworkView can register with the same ID
    //                called before OnNetworkAttach (which is what you probably want to use instead!)
    public virtual void OnNetworkObjectIdAssigned()
    {
    }

    // KEEPSAKE FIX
    public virtual void OnNetworkAttach()
    {
    }

    internal void InternalOnNetworkAttach()
    {
        InitializeVariables();
    }

    /// <summary>
    ///     Gets called when the <see cref="NetworkObject" /> gets spawned, message handlers are ready to be registered and the
    ///     network is setup.
    /// </summary>
    public virtual void OnNetworkSpawn()
    {
    }

    /// <summary>
    ///     Gets called when the <see cref="NetworkObject" /> gets despawned. Is called both on the server and clients.
    /// </summary>
    public virtual void OnNetworkDespawn()
    {
    }

    internal void InternalOnNetworkSpawn()
    {
        //InitializeVariables();
    }

    internal void InternalOnNetworkDespawn()
    {
    }

    /// <summary>
    ///     Gets called when the local client gains ownership of this object
    /// </summary>
    public virtual void OnGainedOwnership()
    {
    }

    /// <summary>
    ///     Gets called when we loose ownership of this object
    /// </summary>
    public virtual void OnLostOwnership()
    {
    }

    // KEEPSAKE FIX - Gets called when owner is changed, even if we're neither the old or new owner
    public virtual void OnOwnershipChanged(ulong oldOwner, ulong newOwner)
    {
    }
    // END KEEPSAKE FIX

    /// <summary>
    ///     Gets called when the parent NetworkObject of this NetworkBehaviour's NetworkObject has changed
    /// </summary>
    public virtual void OnNetworkObjectParentChanged(NetworkObject parentNetworkObject)
    {
    }

    private static FieldInfo[] GetFieldInfoForType(Type type)
    {
        if (!s_FieldTypes.ContainsKey(type))
        {
            s_FieldTypes.Add(type, GetFieldInfoForTypeRecursive(type));
        }

        return s_FieldTypes[type];
    }

    private static FieldInfo[] GetFieldInfoForTypeRecursive(Type type, List<FieldInfo> list = null)
    {
        if (list == null)
        {
            list = new List<FieldInfo>();
            list.AddRange(type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance));
        }
        else
        {
            list.AddRange(type.GetFields(BindingFlags.NonPublic | BindingFlags.Instance));
        }

        if (type.BaseType != null && type.BaseType != typeof(NetworkBehaviour))
        {
            return GetFieldInfoForTypeRecursive(type.BaseType, list);
        }

        return list.OrderBy(x => x.Name, StringComparer.Ordinal).ToArray();
    }

    // KEEPSAKE FIX - see below
    protected virtual void BeforeVarInit()
    {
    }

    // KEEPSAKE FIX - made protected internal to access from our subclass tracking Blackboards
    protected internal void InitializeVariables()
    {
        if (m_VarInit)
        {
            return;
        }

        // KEEPSAKE FIX - InitializeVariables might be called before Awake (e.g. Spawned in disabled parent) so we've added this to give subclasses a chance to initialize
        BeforeVarInit();

        m_VarInit = true;

        if (!gameObject.activeInHierarchy && NetworkManager != null && !NetworkManager.ShutdownInProgress)
        {
            m_VarInitWhileInactive = true;
            NetworkManager.StartTrackingInactiveInitializedBehaviour(this);
        }

        var sortedFields = GetFieldInfoForType(GetType());

        for (var i = 0; i < sortedFields.Length; i++)
        {
            var fieldType = sortedFields[i].FieldType;

            if (fieldType.IsSubclassOf(typeof(NetworkVariableBase)))
            {
                var instance = (NetworkVariableBase)sortedFields[i].GetValue(this);

                if (instance == null)
                {
                    throw new Exception($"{GetType().FullName}.{sortedFields[i].Name} cannot be null. All {nameof(NetworkVariableBase)} instances must be initialized.");
                }

                instance.Initialize(this);

                var instanceNameProperty = fieldType.GetProperty(nameof(NetworkVariableBase.Name));
                var sanitizedVariableName = sortedFields[i].Name.Replace("<", string.Empty).Replace(">k__BackingField", string.Empty);
                instanceNameProperty?.SetValue(instance, sanitizedVariableName);

                NetworkVariableFields.Add(instance);
                StartObservingDirty(instance);
            }
        }

        {
            // Create index map for delivery types
            var firstLevelIndex = new Dictionary<NetworkDelivery, int>();
            var secondLevelCounter = 0;

            for (var i = 0; i < NetworkVariableFields.Count; i++)
            {
                var networkDelivery = NetworkVariableBase.Delivery;
                if (!firstLevelIndex.ContainsKey(networkDelivery))
                {
                    firstLevelIndex.Add(networkDelivery, secondLevelCounter);
                    m_DeliveryTypesForNetworkVariableGroups.Add(networkDelivery);
                    secondLevelCounter++;
                }

                if (firstLevelIndex[networkDelivery] >= m_DeliveryMappedNetworkVariableIndices.Count)
                {
                    m_DeliveryMappedNetworkVariableIndices.Add(new HashSet<int>());
                }

                m_DeliveryMappedNetworkVariableIndices[firstLevelIndex[networkDelivery]].Add(i);
            }
        }
    }

    protected void StartObservingDirty(NetworkVariableBase netVar)
    {
        // We .Skip(1) to avoid getting a callback from the Subscribe and instead check if IsDirty to start out with a count of +1
        NetworkVariableDirtySubscriptions.Add(netVar.WhenDirty.Skip(1).SubscribeWithState(netVar, OnVariableDirty));
        if (netVar.IsDirty())
        {
            OnVariableDirty(true, netVar);
        }
    }

    // KEEPSAKE FIX - extracted from OnDestroy since we started initializing variables for inactive objects, which don't get OnDestroy invoked
    internal void DisposeVariables()
    {
        if (!m_VarInit)
        {
            return;
        }

        for (var i = 0; i < NetworkVariableFields.Count; i++)
        {
            var networkVariableField = NetworkVariableFields[i];
            if (networkVariableField.IsInitialized)
            {
                networkVariableField.Dispose();
            }

            NetworkVariableDirtySubscriptions[i].Dispose();
        }

        m_VarInitWhileInactive = false;
    }

    // KEEPSAKE FIX - Observable IsDirty
    protected void OnVariableDirty(bool isDirty, NetworkVariableBase networkVariableBase)
    {
        m_DirtyVariableCount += isDirty ? 1 : -1;
    }

    internal void PreNetworkVariableWrite()
    {
        // reset our "which variables got written" data
        NetworkVariableIndexesToReset.Clear();
        NetworkVariableIndexesToResetSet.Clear();
    }

    internal void PostNetworkVariableWrite()
    {
        // mark any variables we wrote as no longer dirty
        for (var i = 0; i < NetworkVariableIndexesToReset.Count; i++)
        {
            var networkVariableField = NetworkVariableFields[NetworkVariableIndexesToReset[i]];
            networkVariableField.ResetDirty();

            // KEEPSAKE FIX - track when we're allowed to send based on min delay
            var currentTick = NetworkManager.NetworkTickSystem.LocalTime.Tick;
            var nextTickToAllowSend = currentTick + Mathf.CeilToInt(networkVariableField.MinSendDelay * NetworkManager.NetworkTickSystem.LocalTime.TickRate);
            networkVariableField.MinNextTickWrite = nextTickToAllowSend;
        }
    }

    internal void VariableUpdate(ulong clientId)
    {
        if (!m_VarInit)
        {
            InitializeVariables();
        }

        PreNetworkVariableWrite();
        // KEEPSAKE FIX - get behaviour ID from raw field to avoid property, this is a hot path
        var behaviourId = m_NetworkObject.GetNetworkBehaviourOrderIndex(this);
        NetworkVariableUpdate(clientId, behaviourId);
    }

    private void NetworkVariableUpdate(ulong clientId, ushort behaviourIndex)
    {
        if (!CouldHaveDirtyNetworkVariables())
        {
            return;
        }

        if (NetworkManager.NetworkConfig.UseSnapshotDelta)
        {
            for (var k = 0; k < NetworkVariableFields.Count; k++)
            {
                var networkVariableField = NetworkVariableFields[k];
                // KEEPSAKE FIX - ensure min delay has passed if set
                var currentTick = NetworkManager.NetworkTickSystem.LocalTime.Tick;
                if (networkVariableField.IsDirty() && currentTick >= networkVariableField.MinNextTickWrite)
                {
                    var update = new UpdateCommand();

                    update.NetworkObjectId = NetworkObjectId;
                    update.BehaviourIndex = behaviourIndex;
                    update.VariableIndex = k;

                    NetworkManager.SnapshotSystem.Store(update, networkVariableField);
                    NetworkVariableIndexesToReset.Add(k);

                    NetworkManager.NetworkMetrics.TrackNetworkVariableDeltaSent(clientId, GetNetworkObject(NetworkObjectId), networkVariableField.Name, __getTypeName(), 20); // todo: what length ?
                }
            }
        }

        if (!NetworkManager.NetworkConfig.UseSnapshotDelta)
        {
            // KEEPSAKE FIX - we've made changes to the snapshot system to e.g. support nested NOs, we'd lose that support if using NetworkVariableDeltaMessage instead
            Debug.LogError($"Expected {nameof(NetworkManager.NetworkConfig.UseSnapshotDelta)} to be true!");
            /*for (var j = 0; j < m_DeliveryMappedNetworkVariableIndices.Count; j++)
            {
                var shouldSend = false;
                for (var k = 0; k < NetworkVariableFields.Count; k++)
                {
                    if (NetworkVariableFields[k].ShouldWrite(clientId, IsServer))
                    {
                        shouldSend = true;
                    }
                }

                if (shouldSend)
                {
                    var message = new NetworkVariableDeltaMessage
                    {
                        NetworkObjectId = NetworkObjectId,
                        NetworkBehaviourIndex = NetworkObject.GetNetworkBehaviourOrderIndex(this),
                        NetworkBehaviour = this,
                        ClientId = clientId,
                        DeliveryMappedNetworkVariableIndex = m_DeliveryMappedNetworkVariableIndices[j],
                    };
                    // TODO: Serialization is where the IsDirty flag gets changed.
                    // Messages don't get sent from the server to itself, so if we're host and sending to ourselves,
                    // we still have to actually serialize the message even though we're not sending it, otherwise
                    // the dirty flag doesn't change properly. These two pieces should be decoupled at some point
                    // so we don't have to do this serialization work if we're not going to use the result.
                    if (IsServer && clientId == NetworkManager.ServerClientId)
                    {
                        var tmpWriter = new FastBufferWriter(MessagingSystem.NON_FRAGMENTED_MESSAGE_MAX_SIZE, Allocator.Temp, MessagingSystem.FRAGMENTED_MESSAGE_MAX_SIZE);
                        using (tmpWriter)
                        {
                            message.Serialize(tmpWriter);
                        }
                    }
                    else
                    {
                        NetworkManager.SendMessage(ref message, m_DeliveryTypesForNetworkVariableGroups[j], clientId);
                    }
                }
            }*/
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CouldHaveDirtyNetworkVariables()
    {
        // KEEPSAKE FIX - observable IsDirty
        return m_DirtyVariableCount > 0;
    }

    internal void MarkVariablesDirty()
    {
        for (var j = 0; j < NetworkVariableFields.Count; j++)
        {
            NetworkVariableFields[j].SetDirty(true);
        }
    }

    internal void WriteNetworkVariableData(FastBufferWriter writer, ulong clientId)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"WriteNetworkVariableData: {NetworkVariableFields.Count} field(s) on {NetworkObjectId} / {gameObject.name}");

        if (NetworkVariableFields.Count == 0)
        {
            LogNetcode.Debug(sb.ToString());
            return;
        }

        for (var j = 0; j < NetworkVariableFields.Count; j++)
        {
            var canClientRead = NetworkVariableFields[j].CanClientRead(clientId);
            sb.AppendLine($"- [{j}]: {NetworkVariableFields[j].Name} (can client {clientId} read? {canClientRead})");

            if (canClientRead)
            {
                var writePos = writer.Position;
                writer.WriteValueSafe((ushort)0);
                var startPos = writer.Position;
                NetworkVariableFields[j].WriteField(writer);
                var size = writer.Position - startPos;
                writer.Seek(writePos);
                writer.WriteValueSafe((ushort)size);
                writer.Seek(startPos + size);
            }
            else
            {
                writer.WriteValueSafe((ushort)0);
            }
        }

        LogNetcode.Debug(sb.ToString());
    }

    internal void SetNetworkVariableData(FastBufferReader reader)
    {
        //var sb = new StringBuilder();
        //sb.AppendLine($"SetNetworkVariableData: {NetworkVariableFields.Count} field(s) on {NetworkObjectId} / {gameObject.name}");

        if (NetworkVariableFields.Count == 0)
        {
            return;
        }

        for (var j = 0; j < NetworkVariableFields.Count; j++)
        {
            //sb.AppendLine($"- [{j}]: {NetworkVariableFields[j].Name}");

            reader.ReadValueSafe(out ushort varSize);
            if (varSize == 0)
            {
                continue;
            }

            var readStartPos = reader.Position;
            try
            {
                IsInternalVariableWrite = true;
                NetworkVariableFields[j].ReadField(reader);
            }
            finally
            {
                IsInternalVariableWrite = false;
            }

            if (NetworkManager.NetworkConfig.EnsureNetworkVariableLengthSafety)
            {
                if (reader.Position > readStartPos + varSize)
                {
                    if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
                    {
                        NetworkLog.LogWarning($"Var data read too far. {reader.Position - (readStartPos + varSize)} bytes.");
                    }

                    reader.Seek(readStartPos + varSize);
                }
                else if (reader.Position < readStartPos + varSize)
                {
                    if (NetworkLog.CurrentLogLevel <= LogLevel.Normal)
                    {
                        NetworkLog.LogWarning($"Var data read too little. {readStartPos + varSize - reader.Position} bytes.");
                    }

                    reader.Seek(readStartPos + varSize);
                }
            }
        }

        //LogNetcode.Debug(sb.ToString());
    }

    /// <summary>
    ///     Gets the local instance of a object with a given NetworkId
    /// </summary>
    /// <param name="networkId"></param>
    /// <returns></returns>
    protected NetworkObject GetNetworkObject(ulong networkId)
    {
        // KEEPSAKE FIX - look in tracked objects instead, all spawned objects are also tracked
        return NetworkManager.Singleton.SpawnManager.AttachedObjects.TryGetValue(networkId, out var netObject) ? netObject : null;
        //return NetworkManager.SpawnManager.SpawnedObjects.TryGetValue(networkId, out NetworkObject networkObject) ? networkObject : null;
    }
    #pragma warning disable IDE1006 // disable naming rule violation check
    // RuntimeAccessModifiersILPP will make this `protected`
    internal enum __RpcExecStage
        #pragma warning restore IDE1006 // restore naming rule violation check
    {
        None   = 0,
        Server = 1,
        Client = 2,
    }
}

}
