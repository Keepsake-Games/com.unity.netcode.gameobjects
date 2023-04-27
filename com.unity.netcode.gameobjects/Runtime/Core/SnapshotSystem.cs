using System;
using System.Buffers;
using System.Collections.Generic;
using Cysharp.Threading.Tasks;
using Keepsake.Common;
using UniRx;
using UniRx.Observers;
using Unity.Collections;
using UnityEngine;
using UnityEngine.Assertions;
using UnityEngine.Pool;

// SnapshotSystem stores:
//
// - Spawn, Despwan commands (done)
// - NetworkVariable value updates (todo)
// - RPC commands (todo)
//
// and sends a SnapshotDataMessage every tick containing all the un-acknowledged commands.
//
// SnapshotSystem can function even if some messages are lost. It provides eventual consistency.
// The client receiving a message will get a consistent state for a given tick, but possibly not every ticks
// Reliable RPCs will be guaranteed, unreliable ones not
//
// SnapshotSystem relies on the Transport adapter to fragment an arbitrary-sized message into packets
// This comes with a tradeoff. The Transport-level fragmentation is specialized for networking
// but lacks the context that SnapshotSystem has of the meaning of the RPC, Spawns, etc...
// This could be revisited in the future
//
// It also relies on the INetworkMessage interface and MessagingSystem, but deals directly
// with the FastBufferReader and FastBufferWriter to read/write the messages

namespace Unity.Netcode
{

/// <summary>
///     Header information for a SnapshotDataMessage
/// </summary>
internal struct SnapshotHeader
{
    internal int CurrentTick;          // the tick this captures information for
    internal int LastReceivedSequence; // what we are ack'ing
    internal int SpawnCount;           // number of spawn commands included
    internal int DespawnCount;         // number of despawn commands included
    internal int UpdateCount;          // number of update commands included
    internal int SpawnVariableDataSize; // KEEPSAKE FIX - offset at which variable data for Spawn commands start
}

internal struct UpdateCommand
{
    internal ulong  NetworkObjectId;
    internal ushort BehaviourIndex;
    internal int    VariableIndex;

    // snapshot internal
    internal int TickWritten;
    internal int SerializedLength;
}

internal struct UpdateCommandMeta
{
    internal int         Index;     // the index for the index allocator
    internal int         BufferPos; // the allocated position in the buffer
    internal List<ulong> TargetClientIds;

    // KEEPSAKE FIX - put snapshot commands into groups to decide what to send when, e.g. holding most data before player join
    internal byte Group;

    // KEEPSAKE FIX - "unique" id of command, until uint wraps
    public uint Id;

    // KEEPSAKE FIX - for each client this holds which tick the command was actually included in a snapshot, >= command.TickWritten.
    //                null when not included in any snapshot to client yet
    //                IMPORTANT: Index of client ID in TargetClientIds used for lookup in this list so they must match
    internal List<int?> FirstTicksIncluded;

    // KEEPSAKE FIX - only set locally on server, used for debugging and logging
    public NetworkVariableBase NetworkVariable { get; set; }
}

/// <summary>
///     A command to despawn an object
///     Which object it is, and the tick at which it was despawned
/// </summary>
internal struct SnapshotDespawnCommand
{
    // identity
    internal ulong NetworkObjectId;

    // snapshot internal
    internal int TickWritten;

    // KEEPSAKE FIX - notify client if object is being destroyed to prevent multiple executions of network despawn and/or parent destroy events for children.
    internal bool IsBeingDestroyed;
    // END KEEPSAKE FIX
}

/// <summary>
///     A command to spawn an object
///     Which object it is, what type it has, the spawn parameters and the tick at which it was spawned
/// </summary>
internal struct SnapshotSpawnCommand
{
    // identity
    internal ulong NetworkObjectId;

    // KEEPSAKE FIX - set to >0 when spawned as nested to another NetworkObject
    internal ulong RootNetworkObjectId;

    // archetype
    internal uint GlobalObjectIdHash;
    internal bool IsSceneObject; //todo: how is this unused ?

    // parameters
    internal bool       IsPlayerObject;
    internal ulong      OwnerClientId;
    internal ulong      ParentNetworkId;
    internal bool       WaitForParentIfMissing; // KEEPSAKE FIX - true if parent is attached but lifetime managed not by Netcode (= not Spawned)
    internal Vector3    ObjectPositionLocal;
    internal Quaternion ObjectRotationLocal;
    internal Vector3    ObjectScale; //todo: how is this unused ?

    // internal
    internal int TickWritten;
}

/// <summary>
///     Stores supplemental meta-information about a Spawn or Despawn command.
///     This part doesn't get sent, so is stored elsewhere in order to allow writing just the SnapshotSpawnCommand
/// </summary>
internal struct SnapshotSpawnDespawnCommandMeta
{
    // The remaining clients a command still has to be sent to
    internal List<ulong> TargetClientIds;

    // KEEPSAKE FIX - put snapshot commands into groups to decide what to send when, e.g. holding most data before player join
    internal byte Group;

    // KEEPSAKE FIX - "unique" id of command, until uint wraps
    public uint Id;

    // KEEPSAKE FIX - for each client this holds which tick the command was actually included in a snapshot, >= command.TickWritten.
    //                null when not included in any snapshot to client yet
    //                IMPORTANT: Index of client ID in TargetClientIds used for lookup in this list so they must match
    internal List<int?> FirstTicksIncluded;
}

// KEEPSAKE FIX
internal struct PendingSpawnCommandData
{
    internal SnapshotSpawnCommand Command;

    /// <summary>
    /// PendingSpawnCommand HAS OWNERSHIP of this buffer and should return it to pool when done
    /// </summary>
    internal byte[] NetVarBuffer;
}

/// <summary>
///     Stores information about a specific client.
///     What tick they ack'ed, for now.
/// </summary>
internal struct ClientData
{
    internal int LastReceivedTick; // the last tick received by this client

    // KEEPSAKE FIX - snapshot groups
    internal byte CurrentSnapshotGroup;

    internal ClientData(int unused)
    {
        LastReceivedTick = -1;
        CurrentSnapshotGroup = 0;
    }
}

internal delegate int SendMessageHandler(SnapshotDataMessage message, ulong clientId);

internal delegate UniTask SpawnObjectHandler(SnapshotSpawnCommand spawnCommand, ulong srcClientId, byte[] netVarBuffer);

internal delegate void DespawnObjectHandler(SnapshotDespawnCommand despawnCommand, ulong srcClientId);

internal delegate void GetBehaviourVariableHandler(UpdateCommand updateCommand, out NetworkBehaviour behaviour, out NetworkVariableBase variable, ulong srcClientId);

// KEEPSAKE FIX - made public
public class SnapshotSystem : INetworkUpdateSystem, IDisposable
{
    internal DespawnObjectHandler DespawnObject;

    // This arrays contains all the despawn commands received by the game code.
    // This part can be written as-is to the message.
    // Those are cleaned-up once the despawns are ack'ed by all target clients
    internal SnapshotDespawnCommand[] Despawns;

    // Meta-information about Despawns. Entries are matched by index
    internal SnapshotSpawnDespawnCommandMeta[] DespawnsMeta;
    internal GetBehaviourVariableHandler       GetBehaviourVariable;
    internal byte[]                            MemoryBuffer = new byte[TotalBufferMemory];

    internal IndexAllocator MemoryStorage = new(TotalBufferMemory, TotalMaxIndices);
    internal uint           NextDespawnId; // KEEPSAKE FIX
    internal uint           NextSpawnId;   // KEEPSAKE FIX

    internal uint NextUpdateId; // KEEPSAKE FIX

    // Number of spawns used in the array. The array might actually be bigger, as it reserves space for performance reasons
    internal int NumDespawns;

    // Number of spawns used in the array. The array might actually be bigger, as it reserves space for performance reasons
    internal int NumSpawns;
    internal int NumUpdates;

    internal SendMessageHandler SendMessage;
    internal SpawnObjectHandler SpawnObjectAsync;

    // This arrays contains all the spawn commands received by the game code.
    // This part can be written as-is to the message.
    // Those are cleaned-up once the spawns are ack'ed by all target clients
    internal SnapshotSpawnCommand[] Spawns;

    // Meta-information about Spawns. Entries are matched by index
    internal SnapshotSpawnDespawnCommandMeta[] SpawnsMeta;

    internal Dictionary<ulong, int> TickAppliedDespawn = new();

    // Local state. Stores which spawns and despawns were applied locally
    // indexed by ObjectId
    internal Dictionary<ulong, int> TickAppliedSpawn = new();

    internal UpdateCommand[]     Updates;
    internal UpdateCommandMeta[] UpdatesMeta;

    private readonly int[] m_AvailableIndices;                              // The IndexAllocator indices for memory management
    private readonly int   m_AvailableIndicesBufferCount = TotalMaxIndices; // Size of the buffer storing indices

    private readonly Dictionary<ulong, ClientData> m_ClientData = new();

    // todo: how is this unused and does it belong here ?
    private Dictionary<ulong, ConnectionRtt> m_ConnectionRtts = new();

    // The tick we're currently processing (or last we processed, outside NetworkUpdate())
    private int m_CurrentTick = NetworkTickSystem.NoTick;
    // END KEEPSAKE FIX

    private readonly NetworkManager    m_NetworkManager;
    private readonly NetworkTickSystem m_NetworkTickSystem;
    private          int               m_NumAvailableIndices = TotalMaxIndices; // Current number of valid indices in m_AvailableIndices

    private FastBufferWriter m_Writer;

    // KEEPSAKE FIX
    private readonly Dictionary<ulong, PendingUpdateCommandsData> m_PendingUpdateCommands = new();

    // KEEPSAKE FIX
    private readonly Dictionary<ulong, List<PendingSpawnCommandData>> m_PendingNestedSpawnCommands = new();

    // KEEPSAKE FIX
    private readonly HashSet<ulong> m_PendingDespawnCommands = new();

    // Settings
    internal bool        IsServer           { get; set; }
    internal bool        IsConnectedClient  { get; set; }
    internal ulong       ServerClientId     { get; set; }
    internal List<ulong> ConnectedClientsId { get; } = new();

    // Property showing visibility into inner workings, for testing
    internal int SpawnsBufferCount   { get; private set; } = 100;
    internal int DespawnsBufferCount { get; private set; } = 100;

    internal int UpdatesBufferCount { get; private set; } = 100;

    #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
    // KEEPSAKE FIX - since we started supporting including net var data in spawn commands, add a marker to early crash on malformed spawn commands
    private static readonly byte[] k_MarkerSpawnCommandNetVarBoundary = { 0xc0, 0xff, 0xee };
    #endif

    internal SnapshotSystem(NetworkManager networkManager, NetworkConfig config, NetworkTickSystem networkTickSystem)
    {
        m_NetworkManager = networkManager;
        m_NetworkTickSystem = networkTickSystem;

        m_Writer = new FastBufferWriter(TotalBufferMemory, Allocator.Persistent);

        if (networkManager != null)
        {
            // If we have a NetworkManager, let's send on the network. This can be overriden for tests
            SendMessage = NetworkSendMessage;
            // If we have a NetworkManager, let's (de)spawn with the rest of our package. This can be overriden for tests
            SpawnObjectAsync = NetworkSpawnObjectAsync;
            DespawnObject = NetworkDespawnObject;
            GetBehaviourVariable = NetworkGetBehaviourVariable;
        }

        // register for updates in EarlyUpdate
        this.RegisterNetworkUpdate(NetworkUpdateStage.EarlyUpdate);

        Spawns = new SnapshotSpawnCommand[SpawnsBufferCount];
        SpawnsMeta = new SnapshotSpawnDespawnCommandMeta[SpawnsBufferCount];
        Despawns = new SnapshotDespawnCommand[DespawnsBufferCount];
        DespawnsMeta = new SnapshotSpawnDespawnCommandMeta[DespawnsBufferCount];
        Updates = new UpdateCommand[UpdatesBufferCount];
        UpdatesMeta = new UpdateCommandMeta[UpdatesBufferCount];

        m_AvailableIndices = new int[m_AvailableIndicesBufferCount];
        for (var i = 0; i < m_AvailableIndicesBufferCount; i++)
        {
            m_AvailableIndices[i] = i;
        }

        // KEEPSAKE FIX
        m_NetworkManager.SpawnManager.WhenObjectAttached.Subscribe(OnObjectAttached).UnsubscribeOnDestroy(m_NetworkManager);
    }

    private void OnObjectAttached(NetworkObject networkObject)
    {
        // KEEPSAKE FIX - apply pending commands received while async spawn was working
        var updateCommandSize = FastBufferWriter.GetWriteSize<UpdateCommand>();
        if (m_PendingUpdateCommands.TryGetValue(networkObject.NetworkObjectId, out var pendingUpdates))
        {
            // Net vars will be updated when this is disposed. We do this manually so we can ensure that behaviours are flagged that this is an internal write
            // so that the modification to the network replicated ReactiveProperties does not trigger a disallowed warning
            var propertyNotificationSuspension = ReactivePropertyNotifications.Suspend();
            var behaviours = new List<NetworkBehaviour>();

            try
            {
                LogNetcode.Debug(
                    $"Network Object {networkObject.NetworkObjectId} spawned and has pending updates worth {pendingUpdates.Writer.Length} byte(s) ({pendingUpdates.Headers.Count} separate entries), applying now");
                var reader = new FastBufferReader(pendingUpdates.Writer, Allocator.None);
                while (reader.Length - reader.Position >= updateCommandSize)
                {
                    var startPos = reader.Position;
                    reader.ReadValueSafe(out int headerIndex);
                    //LogNetcode.Debug($"Read header index {headerIndex}");
                    var header = pendingUpdates.Headers[headerIndex];
                    reader.ReadValueSafe(out UpdateCommand updateCommand); // header

                    //LogNetcode.Debug($"Processing previously stashed update command for V#{updateCommand.VariableIndex} B#{updateCommand.BehaviourIndex} NO#{updateCommand.NetworkObjectId} part of tick {updateCommand.TickWritten}");

                    GetBehaviourVariable(updateCommand, out var behaviour, out var variable, header.ClientId);
                    behaviours.Add(behaviour);

                    // will read payload and progress the buffer
                    ProcessUpdateCommand(header.ClientId, reader, updateCommand, variable, behaviour);

                    //LogNetcode.Debug($" .. read and processed update command in buffer at {startPos}-{reader.Position}, {reader.Length - reader.Position} byte(s) remain.");
                }

                m_PendingUpdateCommands.Remove(networkObject.NetworkObjectId);
                ListPool<PendingUpdateCommandHeader>.Release(pendingUpdates.Headers);
                pendingUpdates.Headers = null;
                pendingUpdates.Writer.Dispose();
            }
            finally
            {
                try
                {
                    foreach (var behaviour in behaviours)
                    {
                        behaviour.IsInternalVariableWrite = true;
                    }

                    propertyNotificationSuspension.Dispose();
                }
                finally
                {
                    foreach (var behaviour in behaviours)
                    {
                        behaviour.IsInternalVariableWrite = false;
                    }
                }
            }
        }
    }

    public void Dispose()
    {
        this.UnregisterNetworkUpdate(NetworkUpdateStage.EarlyUpdate);

        // KEEPSAKE FIX
        foreach (var kvp in m_PendingUpdateCommands)
        {
            var pendingUpdates = kvp.Value;
            ListPool<PendingUpdateCommandHeader>.Release(pendingUpdates.Headers);
            pendingUpdates.Headers = null;
            pendingUpdates.Writer.Dispose();
        }
        m_PendingUpdateCommands.Clear();
    }

    public void NetworkUpdate(NetworkUpdateStage updateStage)
    {
        if (updateStage == NetworkUpdateStage.EarlyUpdate)
        {
            UpdateClientServerData();

            var tick = m_NetworkTickSystem.LocalTime.Tick;

            if (tick != m_CurrentTick)
            {
                m_CurrentTick = tick;
                if (IsServer)
                {
                    for (var i = 0; i < ConnectedClientsId.Count; i++)
                    {
                        var clientId = ConnectedClientsId[i];

                        // don't send to ourselves
                        if (clientId != ServerClientId)
                        {
                            SendSnapshot(clientId);
                        }
                    }
                }
                else if (IsConnectedClient)
                {
                    SendSnapshot(ServerClientId);
                }
            }
        }
    }

    internal const int TotalMaxIndices   = 1000;
    internal const int TotalBufferMemory = 20 * 1024 * 1024; // KEEPSAKE FIX - more memory

    // returns the default client list: just the server, on clients, all clients, on the server
    internal List<ulong> GetClientList()
    {
        List<ulong> clientList;
        clientList = new List<ulong>();

        if (!IsServer)
        {
            clientList.Add(m_NetworkManager.ServerClientId);
        }
        else
        {
            foreach (var clientId in ConnectedClientsId)
            {
                if (clientId != m_NetworkManager.ServerClientId)
                {
                    clientList.Add(clientId);
                }
            }
        }

        return clientList;
    }

    /// <summary>
    ///     Shrink the buffer to the minimum needed. Frees the reserved space.
    ///     Mostly for testing at the moment, but could be useful for game code to reclaim memory
    /// </summary>
    internal void ReduceBufferUsage()
    {
        var count = Math.Max(1, NumDespawns);
        Array.Resize(ref Despawns, count);
        DespawnsBufferCount = count;

        count = Math.Max(1, NumSpawns);
        Array.Resize(ref Spawns, count);
        SpawnsBufferCount = count;
    }

    /// <summary>
    ///     Called by SnapshotSystem, to spawn an object locally
    ///     todo: consider observer pattern
    /// </summary>
    /// KEEPSAKE FIX - support for including variable data in spawn command
    internal async UniTask NetworkSpawnObjectAsync(SnapshotSpawnCommand spawnCommand, ulong srcClientId, byte[] netVarBuffer)
    {
        try
        {
            //Debug.Log($"PICKUPABLE SPAWN DEBUG -- {nameof(SnapshotSystem)}.{nameof(NetworkSpawnObjectAsync)} processing spawn command for NO #{spawnCommand.NetworkObjectId} ({spawnCommand.GlobalObjectIdHash}). Position {spawnCommand.ObjectPositionLocal} in relation to {(spawnCommand.ParentNetworkId == spawnCommand.NetworkObjectId ? "WORLD (has no parent)" : $"parent NO #{spawnCommand.ParentNetworkId}")}");
            //LogNetcode.Debug($"Handling Spawn command for NO #{spawnCommand.NetworkObjectId} ({spawnCommand.GlobalObjectIdHash})...");

            NetworkObject networkObject;
            if (spawnCommand.ParentNetworkId == spawnCommand.NetworkObjectId)
            {
                // KEEPSAKE FIX - use sceneObject from the spawn command instead of always being false. Even scene objects are spawned as a way to hook them up.
                networkObject = await m_NetworkManager.SpawnManager.CreateLocalNetworkObjectAsync(
                    spawnCommand.IsSceneObject,
                    spawnCommand.GlobalObjectIdHash,
                    spawnCommand.OwnerClientId,
                    null,
                    spawnCommand.RootNetworkObjectId == 0 ? null : spawnCommand.RootNetworkObjectId,
                    false,
                    spawnCommand.ObjectPositionLocal,
                    spawnCommand.ObjectRotationLocal);
            }
            else
            {
                // KEEPSAKE FIX - use sceneObject from the spawn command instead of always being false. Even scene objects are spawned as a way to hook them up.
                networkObject = await m_NetworkManager.SpawnManager.CreateLocalNetworkObjectAsync(
                    spawnCommand.IsSceneObject,
                    spawnCommand.GlobalObjectIdHash,
                    spawnCommand.OwnerClientId,
                    spawnCommand.ParentNetworkId,
                    spawnCommand.RootNetworkObjectId == 0 ? null : spawnCommand.RootNetworkObjectId,
                    spawnCommand.WaitForParentIfMissing,
                    spawnCommand.ObjectPositionLocal,
                    spawnCommand.ObjectRotationLocal);
            }

            // KEEPSAKE FIX
            if (networkObject == null)
            {
                LogNetcode.Error($"Failed to create local network object in {nameof(NetworkSpawnObjectAsync)}. {nameof(spawnCommand.IsSceneObject)}? {spawnCommand.IsSceneObject} -- {nameof(spawnCommand.GlobalObjectIdHash)} {spawnCommand.GlobalObjectIdHash} -- {nameof(spawnCommand.RootNetworkObjectId)} {spawnCommand.RootNetworkObjectId}");
                return;
            }

            // KEEPSAKE FIX - support for including variable data in spawn command
            if (netVarBuffer != null)
            {
                var variableData = new FastBufferReader(netVarBuffer, Allocator.Temp);
                networkObject.SetNetworkVariableData(variableData, spawnCommand.TickWritten);
            }

            // KEEPSAKE FIX - use sceneObject from the spawn command instead of always being true. Even scene objects are spawned as a way to hook them up.
            m_NetworkManager.SpawnManager.SpawnNetworkObjectLocally(
                networkObject,
                spawnCommand.NetworkObjectId,
                spawnCommand.IsSceneObject,
                spawnCommand.IsPlayerObject,
                spawnCommand.OwnerClientId,
                false);

            // KEEPSAKE FIX - spawn any pending nested that might've arrived
            if (m_PendingNestedSpawnCommands.TryGetValue(networkObject.NetworkObjectId, out var pendingSpawns))
            {
                foreach (var ps in pendingSpawns)
                {
                    await NetworkSpawnObjectAsync(ps.Command, srcClientId, ps.NetVarBuffer);
                }
                m_PendingNestedSpawnCommands.Remove(networkObject.NetworkObjectId);
            }

            //todo: discuss with tools how to report shared bytes
            m_NetworkManager.NetworkMetrics.TrackObjectSpawnReceived(srcClientId, networkObject, 8);

            if (m_PendingDespawnCommands.Remove(networkObject.NetworkObjectId))
            {
                m_NetworkManager.SpawnManager.OnDespawnObject(networkObject, true);
            }
        }
        finally
        {
            if (netVarBuffer != null)
            {
                ArrayPool<byte>.Shared.Return(netVarBuffer);
            }
        }
    }

    /// <summary>
    ///     Called by SnapshotSystem, to despawn an object locally
    /// </summary>
    internal void NetworkDespawnObject(SnapshotDespawnCommand despawnCommand, ulong srcClientId)
    {
        // KEEPSAKE FIX - null check
        if (!m_NetworkManager.SpawnManager.SpawnedObjects.TryGetValue(despawnCommand.NetworkObjectId, out var networkObject))
        {
            if (!TickAppliedSpawn.TryGetValue(despawnCommand.NetworkObjectId, out _))
            {
                LogNetcode.Warning($"Received Despawn command for unknown NetworkObject {despawnCommand.NetworkObjectId}");
            }
            else
            {
                m_PendingDespawnCommands.Add(despawnCommand.NetworkObjectId);
            }
            return;
        }

        // KEEPSAKE FIX - set to replicated value
        networkObject.IsBeingDestroyed = despawnCommand.IsBeingDestroyed;
        // END KEEPSAKE FIX

        m_NetworkManager.SpawnManager.OnDespawnObject(networkObject, true);
        //todo: discuss with tools how to report shared bytes
        m_NetworkManager.NetworkMetrics.TrackObjectDestroyReceived(srcClientId, networkObject, 8);
    }

    /// <summary>
    ///     Updates the internal state of SnapshotSystem to refresh its knowledge of:
    ///     - am I a server
    ///     - what are the client Ids
    ///     todo: consider optimizing
    /// </summary>
    internal void UpdateClientServerData()
    {
        if (m_NetworkManager)
        {
            IsServer = m_NetworkManager.IsServer;
            IsConnectedClient = m_NetworkManager.IsConnectedClient;
            ServerClientId = m_NetworkManager.ServerClientId;

            // todo: This is extremely inefficient. What is the efficient and idiomatic way ?
            ConnectedClientsId.Clear();
            if (IsServer)
            {
                foreach (var id in m_NetworkManager.ConnectedClientsIds)
                {
                    ConnectedClientsId.Add(id);
                }
            }
        }
    }

    internal ConnectionRtt GetConnectionRtt(ulong clientId)
    {
        return new ConnectionRtt();
    }

    // where we build and send a snapshot to a given client
    private void SendSnapshot(ulong clientId)
    {
        var header = new SnapshotHeader();
        var message = new SnapshotDataMessage(0);

        // Verify we allocated client Data for this clientId
        // KEEPSAKE FIX - TryGetValue so we can use it below
        if (!m_ClientData.TryGetValue(clientId, out var clientData))
        {
            clientData = new ClientData(0);
            m_ClientData.Add(clientId, clientData);
        }

        // Find which spawns must be included
        var spawnsToInclude = new List<int>();
        var spawnsVariableDataOffsetAndSerializedLength = new Dictionary<int, (int offset, int serializedLength)>();
        var spawnsVariableData = new FastBufferWriter(message.WriteBuffer.MaxCapacity, Allocator.Temp);

        // KEEPSAKE FIX - clients send snapshots containing Update commands over things they have authority over, they dont send Spawn or Despawn
        if (m_NetworkManager.IsServer)
        {
            for (var index = 0; index < NumSpawns; index++)
            {
                var meta = SpawnsMeta[index];
                var clientIndex = meta.TargetClientIds.IndexOf(clientId);
                if (clientIndex == -1)
                {
                    //Debug.Log($"SPAWN #{meta.Id} SKIP SEND -- {clientId} not recipient -- recipients ({meta.TargetClientIds.Count}): {string.Join(", ", meta.TargetClientIds)}");
                    continue;
                }

                // KEEPSAKE FIX - don't include command if for group client isn't in yet
                if (clientData.CurrentSnapshotGroup < meta.Group)
                {
                    //Debug.Log($"SPAWN #{meta.Id} SKIP SEND -- client {clientId} is in group {clientData.CurrentSnapshotGroup} while command is in group {meta.Group}");
                    continue;
                }

                // KEEPSAKE FIX - already included once, and since snapshots are currently sent reliably no need to include it again
                if (meta.FirstTicksIncluded[clientIndex].HasValue)
                {
                    continue;
                }

                //Debug.Log($"SPAWN #{meta.Id} (at index {index}) Being sent to {clientId} as part of snapshot {m_CurrentTick}");
                spawnsToInclude.Add(index);

                // KEEPSAKE FIX - include variable data in command, we can't do this when SpawnCommand is added to Spawns[] because the variable payload
                //                may be different for different clients, but now that we know which client we are going to send to we can write it
                {
                    var offset = spawnsVariableData.Position;

                    // if the object has been despawned before we've been able to send the Spawn we don't include any variable data since we've lost it
                    // it might be an idea to remove the undelivered Spawn command similar to Updates, when a Despawn is queued (see CleanUpdateFromSnapshot)
                    if (m_NetworkManager.SpawnManager.SpawnedObjects.TryGetValue(Spawns[index].NetworkObjectId, out var no))
                    {
                        no.WriteNetworkVariableData(spawnsVariableData, clientId);

                        // Net vars that should be sent eventually, but are currently in a later Snapshot Group that client, gets marked dirty to be
                        // picked up by SnapshotSystem in later ticks and queued as Updates
                        for (var i = 0; i < no.ChildNetworkBehaviours.Count; i++)
                        {
                            var (behaviour, _) = no.ChildNetworkBehaviours[i];
                            if (behaviour == null)
                            {
                                continue;
                            }

                            for (var j = 0; j < behaviour.NetworkVariableFields.Count; j++)
                            {
                                var canClientReadButNotYet = behaviour.NetworkVariableFields[j].CanClientRead(clientId)
                                    && !behaviour.NetworkVariableFields[j].IsClientInSnapshotGroup(clientId);

                                if (canClientReadButNotYet)
                                {
                                    behaviour.NetworkVariableFields[j].SetDirty(true, clientId);
                                }
                            }
                        }
                    }


                    // boundary markers to more easily detect errors
                    #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
                    if (spawnsVariableData.Position > offset) // don't write marker if we didn't write any variable data
                    {
                        if (!spawnsVariableData.TryBeginWrite(
                                FastBufferWriter.GetWriteSize<int>() + k_MarkerSpawnCommandNetVarBoundary.Length))
                        {
                            throw new OverflowException("Could not serialize spawn command net var boundary marker: Out of buffer space.");
                        }
                        spawnsVariableData.WriteValue(k_MarkerSpawnCommandNetVarBoundary.Length);
                        foreach (var b in k_MarkerSpawnCommandNetVarBoundary)
                        {
                            spawnsVariableData.WriteValue(b);
                        }
                    }
                    #endif

                    var serializedLength = spawnsVariableData.Position - offset;

                    spawnsVariableDataOffsetAndSerializedLength[index] = (offset, serializedLength);
                }
                // END KEEPSAKE FIX

                // KEEPSAKE FIX - track the tick we're actually including the command in
                meta.FirstTicksIncluded[clientIndex] ??= m_CurrentTick;
            }
        }

        // Find which despawns must be included
        var despawnsToInclude = new List<int>();
        // KEEPSAKE FIX - clients send snapshots containing Update commands over things they have authority over, they dont send Spawn or Despawn
        if (m_NetworkManager.IsServer)
        {
            for (var index = 0; index < NumDespawns; index++)
            {
                var meta = DespawnsMeta[index];
                var clientIndex = meta.TargetClientIds.IndexOf(clientId);
                if (clientIndex == -1)
                {
                    //Debug.Log($"DESPAWN #{meta.Id} SKIP SEND -- {clientId} not recipient -- recipients: {string.Join(", ", meta.TargetClientIds)}");
                    continue;
                }

                // KEEPSAKE FIX - don't include command if for group client isn't in yet
                if (clientData.CurrentSnapshotGroup < meta.Group)
                {
                    //Debug.Log($"DESPAWN #{meta.Id} SKIP SEND -- client {clientId} is in group {clientData.CurrentSnapshotGroup} while command is in group {meta.Group}");
                    continue;
                }

                // KEEPSAKE FIX - already included once, and since snapshots are currently sent reliably no need to include it again
                if (meta.FirstTicksIncluded[clientIndex].HasValue)
                {
                    continue;
                }

                //Debug.Log($"DESPAWN #{meta.Id} (at index {index}) Being sent to {clientId} as part of snapshot {m_CurrentTick}");
                despawnsToInclude.Add(index);

                // KEEPSAKE FIX - track the tick we're actually including the command in
                meta.FirstTicksIncluded[clientIndex] ??= m_CurrentTick;
            }
        }

        // Find which value updates must be included
        var updatesToInclude = new List<int>();
        var updatesPayloadLength = 0;
        for (var index = 0; index < NumUpdates; index++)
        {
            var meta = UpdatesMeta[index];
            var clientIndex = meta.TargetClientIds.IndexOf(clientId);
            if (clientIndex == -1)
            {
                //Debug.Log($"UPDATE #{meta.Id} SKIP SEND -- {clientId} not recipient -- recipients: {string.Join(", ", meta.TargetClientIds)}");
                continue;
            }

            // KEEPSAKE FIX - don't include command if for group client isn't in yet, assume server is always ready for any group
            if (clientId != m_NetworkManager.ServerClientId && clientData.CurrentSnapshotGroup < meta.Group)
            {
                //Debug.Log($"UPDATE #{meta.Id} SKIP SEND -- client {clientId} is in group {clientData.CurrentSnapshotGroup} while command is in group {meta.Group}");
                continue;
            }

            // KEEPSAKE FIX - already included once, and since snapshots are currently sent reliably no need to include it again
            if (meta.FirstTicksIncluded[clientIndex].HasValue)
            {
                continue;
            }

            updatesToInclude.Add(index);
            updatesPayloadLength += Updates[index].SerializedLength;

            // KEEPSAKE FIX - track the tick we're actually including the command in
            //Debug.Log($"UPDATE #{meta.Id} at index {index} Being sent to {clientId} as part of snapshot {m_CurrentTick}");
            meta.FirstTicksIncluded[clientIndex] ??= m_CurrentTick;
        }

        // KEEPSAKE FIX - added support for variable data in spawn commands. Total length is data + an int for each entry for SerializedLength
        var spawnsVariableDataLength = spawnsVariableData.Position + spawnsToInclude.Count * FastBufferWriter.GetWriteSize<int>();

        header.CurrentTick = m_CurrentTick;
        header.SpawnCount = spawnsToInclude.Count;
        header.SpawnVariableDataSize = spawnsVariableDataLength;
        header.DespawnCount = despawnsToInclude.Count;
        header.UpdateCount = updatesToInclude.Count;
        header.LastReceivedSequence = m_ClientData[clientId].LastReceivedTick;
        //Debug.Log($"Acking snapshot {header.LastReceivedSequence } when sending snapshot {m_CurrentTick}");
        /*if (header.SpawnCount > 0 || header.DespawnCount > 0 || header.UpdateCount > 0)
        {
            Debug.Log($"Sending snapshot {header.CurrentTick} to peer {clientId} containing S:{header.SpawnCount} D:{header.DespawnCount} U:{header.UpdateCount}");
        }*/

        if (!message.WriteBuffer.TryBeginWrite(FastBufferWriter.GetWriteSize(header)
                + spawnsToInclude.Count * FastBufferWriter.GetWriteSize(Spawns[0])
                + spawnsVariableDataLength // KEEPSAKE FIX
                + despawnsToInclude.Count * FastBufferWriter.GetWriteSize(Despawns[0])
                + updatesToInclude.Count * FastBufferWriter.GetWriteSize(Updates[0])
                + updatesPayloadLength))
        {
            // todo: error handling
            Debug.Assert(false, "Unable to secure buffer for sending");
        }

        message.WriteBuffer.WriteValue(header);

        // Write the Spawns.
        foreach (var index in spawnsToInclude)
        {
            message.WriteBuffer.WriteValue(Spawns[index]);
            // KEEPSAKE FIX - support for including variable data in Spawn commands
            (var variableDataOffset, var variableDataSerializedLength) = spawnsVariableDataOffsetAndSerializedLength[index];
            message.WriteBuffer.WriteValue(in variableDataSerializedLength);
            if (variableDataSerializedLength > 0)
            {
                unsafe
                {
                    message.WriteBuffer.WriteBytes(spawnsVariableData.GetUnsafePtr(), variableDataSerializedLength, variableDataOffset);
                }
            }
        }

        // Write the Updates, interleaved with the variable payload
        foreach (var index in updatesToInclude)
        {
            message.WriteBuffer.WriteValue(Updates[index]);
            message.WriteBuffer.WriteBytes(MemoryBuffer, Updates[index].SerializedLength, UpdatesMeta[index].BufferPos);
        }

        // Write the Despawns.
        foreach (var index in despawnsToInclude)
        {
            message.WriteBuffer.WriteValue(Despawns[index]);
        }

        SendMessage(message, clientId);
    }

    internal void CleanUpdateFromSnapshot(SnapshotDespawnCommand despawnCommand)
    {
        for (var i = 0; i < NumUpdates; /*increment done below*/)
        {
            // if this is a despawn command for an object we have an update for, let's forget it
            if (Updates[i].NetworkObjectId == despawnCommand.NetworkObjectId)
            {
                // KEEPSAKE FIX - delivery tracking
                // track as delivered even when we're forgetting about the update to despawn it
                m_NetworkManager.SnapshotUpdateDelivered?.Invoke(UpdatesMeta[i].Id);

                // deallocate the memory
                MemoryStorage.Deallocate(UpdatesMeta[i].Index);
                // retrieve the index as available
                m_AvailableIndices[m_NumAvailableIndices++] = UpdatesMeta[i].Index;

                ListPool<ulong>.Release(UpdatesMeta[i].TargetClientIds);
                ListPool<int?>.Release(UpdatesMeta[i].FirstTicksIncluded);

                // KEEPSAKE FIX - shift entire collection rather than moving tail to the now empty slot
                //                this is to ensure sending and processing Updates in the order they were queued
                Array.Copy(Updates, i + 1, Updates, i, NumUpdates - 1 - i);
                Array.Copy(UpdatesMeta, i + 1, UpdatesMeta, i, NumUpdates - 1 - i);
                /*Updates[i] = Updates[NumUpdates - 1];
                UpdatesMeta[i] = UpdatesMeta[NumUpdates - 1];*/
                NumUpdates--;

                // skip incrementing i
                continue;
            }

            i++;
        }

        // KEEPSAKE FIX - also forget any pending nested spawns where this is the spawned parent being despawned
        for (var i = 0; i < NumSpawns; /*increment done below*/)
        {
            // if this is a despawn command for an object we have a nested spawn for, let's forget it
            if (Spawns[i].RootNetworkObjectId == despawnCommand.NetworkObjectId)
            {
                // KEEPSAKE FIX - delivery tracking
                // track as delivered even when we're forgetting about the nested spawn to despawn it
                m_NetworkManager.SnapshotSpawnDelivered?.Invoke(SpawnsMeta[i].Id);

                // KEEPSAKE FIX - using ListPool
                ListPool<ulong>.Release(SpawnsMeta[i].TargetClientIds);
                ListPool<int?>.Release(SpawnsMeta[i].FirstTicksIncluded);

                Array.Copy(Spawns, i + 1, Spawns, i, NumSpawns - 1 - i);
                Array.Copy(SpawnsMeta, i + 1, SpawnsMeta, i, NumSpawns - 1 - i);
                NumSpawns--;

                // skip incrementing i
                continue;
            }

            i++;
        }
    }

    /// <summary>
    ///     Entry-point into SnapshotSystem to spawn an object
    ///     called with a SnapshotSpawnCommand, the NetworkObject and a list of target clientIds, where null means all clients
    ///     KEEPSAKE FIX: targetClientIds should be from ListPool and Spawn will TAKE OWNERSHIP OF IT (unless null)
    /// </summary>
    internal void Spawn(SnapshotSpawnCommand command, NetworkObject networkObject, List<ulong> targetClientIds)
    {
        command.TickWritten = m_CurrentTick;

        // KEEPSAKE FIX - poor man's defer
        var releaseTargetClientIdsOnExit = true;
        try
        {

            #if !KEEPSAKE_BUILD_SHIPPING
            foreach (var parentNo in networkObject.gameObject.GetComponentsInParent<NetworkObject>(true))
            {
                if (parentNo != networkObject && parentNo.m_SnapshotGroup > networkObject.m_SnapshotGroup)
                {
                    // Even if parent's spawn command was queued before the child, if the parent is in a higher snapshot group the command will be held until
                    // the receiving client is on that group, which might allow the child spawn command to be delivered first.
                    // We could possibly handle this in runtime and always set child spawn commands' snapshot group of at least that of any ancestor
                    // but that would only hide the problem that something isn't setup properly in the editor.

                    throw new SpawnStateException(
                        $"Tried to send Spawn command for child {networkObject.gameObject.Path(true)} when its NetworkObject parent {parentNo.gameObject.Path(true)} belongs to a higher snapshot group ({parentNo.m_SnapshotGroup})."
                        + $" Children aren't allowed to spawn before their parents!\nIf feasible increase the snapshot group of {networkObject.gameObject.name} to {parentNo.m_SnapshotGroup}, however there's probably a good reason why the group was set to {networkObject.m_SnapshotGroup}"
                        + $" in the first place, so the proper solution is probably to lower the snapshot group of {parentNo.gameObject.name} to <= {networkObject.m_SnapshotGroup} so that it can get spawned first.");
                }
            }
            #endif

            if (NumSpawns >= SpawnsBufferCount)
            {
                SpawnsBufferCount = SpawnsBufferCount * 2;
                Array.Resize(ref Spawns, SpawnsBufferCount);
                Array.Resize(ref SpawnsMeta, SpawnsBufferCount);
            }

            if (targetClientIds == default)
            {
                // KEEPSAKE FIX - get list from pool so we can release it uniformly when Spawn command is done
                targetClientIds = ListPool<ulong>.Get();
                targetClientIds.AddRange(GetClientList());
            }

            // todo:
            // this 'if' might be temporary, but is needed to help in debugging
            // or maybe it stays
            if (targetClientIds.Count > 0)
            {
                var index = NumSpawns;
                Spawns[index] = command;
                var meta = SpawnsMeta[index];
                meta.TargetClientIds = targetClientIds; // takes ownership
                // KEEPSAKE FIX
                meta.FirstTicksIncluded = ListPool<int?>.Get();
                foreach (var _ in meta.TargetClientIds)
                {
                    meta.FirstTicksIncluded.Add(null);
                }
                // END KEEPSAKE FIX

                meta.Group = networkObject.m_SnapshotGroup; // KEEPSAKE FIX
                meta.Id = NextSpawnId++;                    // KEEPSAKE FIX
                SpawnsMeta[index] = meta;

                NumSpawns++;

                // KEEPSAKE FIX we seem to have gotten here without any exceptions, so let SpawnCommand keep ownership of targetClientIds
                releaseTargetClientIdsOnExit = false;

                // KEEPSAKE FIX - delivery tracking
                //Debug.Log($"SPAWN #{meta.Id} QUEUED with index {index} -- NO #{networkObject.NetworkObjectId} {networkObject.gameObject} on tick {command.TickWritten} -- Global Object ID Hash {networkObject.GlobalObjectIdHash} -- Parent NO ID #{command.ParentNetworkId} -- Scene object? {command.IsSceneObject}. Target client(s): {string.Join(", ", meta.TargetClientIds)}");
                m_NetworkManager.SnapshotSpawnQueued?.Invoke(meta.Id);

                if (m_NetworkManager)
                {
                    foreach (var dstClientId in meta.TargetClientIds)
                    {
                        m_NetworkManager.NetworkMetrics.TrackObjectSpawnSent(dstClientId, networkObject, 8);
                    }
                }
            }
        }
        finally
        {
            if (releaseTargetClientIdsOnExit)
            {
                ListPool<ulong>.Release(targetClientIds);
            }
        }
    }

    /// <summary>
    ///     Entry-point into SnapshotSystem to despawn an object
    ///     called with a SnapshotDespawnCommand, the NetworkObject and a list of target clientIds, where null means all
    ///     clients
    ///     KEEPSAKE FIX: targetClientIds should be from ListPool and Spawn till TAKE OWNERSHIP OF IT (unless null)
    /// </summary>
    internal void Despawn(SnapshotDespawnCommand command, NetworkObject networkObject, List<ulong> targetClientIds)
    {
        // KEEPSAKE FIX - early out if any despawn snapshot already has a parent queued. This would otherwise create redundant despawn snapshots since children are automatically destroyed from their parent.
        if (Despawns.Length > 0 && networkObject.transform.parent != null)
        {
            var parentNetworkObjects = networkObject.transform.parent.GetComponentsInParent<NetworkObject>(true);
            foreach (var despawnEntry in Despawns)
            {
                var despawnNetworkObjectId = despawnEntry.NetworkObjectId;
                foreach (var parentNo in parentNetworkObjects)
                {
                    if (parentNo.NetworkObjectId == despawnNetworkObjectId)
                    {
                        return;
                    }
                }
            }
        }
        // KEEPSAKE FIX END

        var releaseTargetClientIdsOnExit = true;
        try
        {
            command.TickWritten = m_CurrentTick;

            if (NumDespawns >= DespawnsBufferCount)
            {
                DespawnsBufferCount = DespawnsBufferCount * 2;
                Array.Resize(ref Despawns, DespawnsBufferCount);
                Array.Resize(ref DespawnsMeta, DespawnsBufferCount);
            }

            if (targetClientIds == default)
            {
                // KEEPSAKE FIX - use pool
                targetClientIds = ListPool<ulong>.Get();
                targetClientIds.AddRange(GetClientList());
            }

            // todo:
            // this 'if' might be temporary, but is needed to help in debugging
            // or maybe it stays
            if (targetClientIds.Count > 0)
            {
                var index = NumDespawns;
                Despawns[index] = command;
                var meta = DespawnsMeta[index];
                meta.TargetClientIds = targetClientIds; // takes ownership
                // KEEPSAKE FIX
                meta.FirstTicksIncluded = ListPool<int?>.Get();
                foreach (var _ in meta.TargetClientIds)
                {
                    meta.FirstTicksIncluded.Add(null);
                }
                // END KEEPSAKE FIX
                meta.Group = networkObject.m_SnapshotGroup; // KEEPSAKE FIX
                meta.Id = NextDespawnId++;                  // KEEPSAKE FIX
                DespawnsMeta[index] = meta;

                // KEEPSAKE FIX
                releaseTargetClientIdsOnExit = false;

                NumDespawns++;

                // KEEPSAKE FIX - delivery tracking
                //Debug.Log($"DESPAWN #{meta.Id} QUEUED with index {index} -- NO #{networkObject.NetworkObjectId} {networkObject.gameObject} on tick {command.TickWritten}");
                m_NetworkManager.SnapshotDespawnQueued?.Invoke(meta.Id);
            }

            CleanUpdateFromSnapshot(command);

            if (m_NetworkManager)
            {
                foreach (var dstClientId in targetClientIds)
                {
                    m_NetworkManager.NetworkMetrics.TrackObjectDestroySent(dstClientId, networkObject, 8);
                }
            }
        }
        finally
        {
            if (releaseTargetClientIdsOnExit)
            {
                ListPool<ulong>.Release(targetClientIds);
            }
        }
    }

    // entry-point for value updates
    // KEEPSAKE FIX - pass along clientId so we can support per-client dirty flags
    internal void Store(UpdateCommand command, NetworkVariableBase networkVariable, ulong clientId)
    {
        command.TickWritten = m_CurrentTick;
        var commandPosition = -1;

        // KEEPSAKE FIX - use provided clientId
        /*var targetClientIds = GetClientList();
        if (targetClientIds.Count == 0)
        {
            return;
        }*/

        // KEEPSAKE TODO - if we're server don't send to owning client if they have authority

        // KEEPSAKE FIX
        // We've changed so that Update commands are only re-used when they are from (1) the same tick and (2) for the same client
        // The reason is that this method is invoked by the following pattern during a Network tick
        // [Network tick] -> [For every client] -> [For every object] -> [For every behaviour] -> [For every dirty variable] -> Store()
        // So that a variable that is dirty for more than 1 client, will get Store invoked for it more than once.
        //
        // We've added the concept of "per-client dirtyness" (as opposed to being dirty, or not, for all) and we've also made Write/ReadDelta methods
        // to have knowledge of which client they're writing deltas for (so that NetworkLists and derived can track and write "dirty events" per-client).
        // This means that Update commands for NetworkLists can never be shared between different clients, since the written payload could be different.
        // For variables this is currently not the case (but who's to say it won't be?) so for a brief period Update commands for NetworkVariables were
        // allowed to be re-used between clients but this can create timing issues, as follows:
        // - On a PlayerCharacter, both m_TrackedBlackboardData and PossessableCharacterData.m_Possessor are flagged as dirty for clients 1 and 2
        // - Network tick happens
        //   - for client 1
        //     - visit m_TrackedBlackboardData and collect its delta (which is that PossessableCharacterData has been added),
        //        Store a new Update command at slot #10.
        //     - visit PossessableCharacterData.m_Possessor and collect its delta, Store a new Update command at slot #11.
        //   - for client 2
        //     - visit m_TrackedBlackboardData and collect its delta, Store a new Update (re-use not allowed for lists) at slot #12.
        //     - visit PossessableCharacterData.m_Posessor and collect its delta, *but* re-use the existing Update command at slot #11.
        // - Now when client 2 receives the snapshot it will get an error, because PossessableCharacterData.m_Possessor delta will come *before*
        //   the m_TrackedBlackboardData delta, which says that PossessableCharacterData is tracked! :boom:

        // Look for an existing variable's position to update before adding a new entry
        for (var i = 0; i < NumUpdates; i++)
        {
            if (Updates[i].BehaviourIndex == command.BehaviourIndex
                && Updates[i].NetworkObjectId == command.NetworkObjectId
                && Updates[i].VariableIndex == command.VariableIndex
                && Updates[i].TickWritten == command.TickWritten
                && UpdatesMeta[i].TargetClientIds[0] == clientId) // KEEPSAKE NOTE: We look at [0] without regrets since Updates always have at least 1 target, and is never shared between more than 1 target
            {
                commandPosition = i;
                break;
            }
        }

        if (commandPosition == -1)
        {
            var index = -1;

            if (NumUpdates >= UpdatesBufferCount)
            {
                UpdatesBufferCount = UpdatesBufferCount * 2;
                Array.Resize(ref Updates, UpdatesBufferCount);
                Array.Resize(ref UpdatesMeta, UpdatesBufferCount);
            }

            commandPosition = NumUpdates;
            NumUpdates++;

            index = m_AvailableIndices[0];
            m_AvailableIndices[0] = m_AvailableIndices[m_NumAvailableIndices - 1];
            m_NumAvailableIndices--;

            UpdatesMeta[commandPosition].Index = index;
            UpdatesMeta[commandPosition].Id = NextUpdateId++;
            UpdatesMeta[commandPosition].NetworkVariable = networkVariable;

            UpdatesMeta[commandPosition].TargetClientIds = ListPool<ulong>.Get();
            UpdatesMeta[commandPosition].FirstTicksIncluded = ListPool<int?>.Get();

            // KEEPSAKE FIX - delivery tracking
            //Debug.Log($"UPDATE to {networkVariable.Name} allocated as new command with ID #{UpdatesMeta[commandPosition].Id} QUEUED at index {commandPosition} -- NO #{command.NetworkObjectId} from tick {command.TickWritten}");
            m_NetworkManager.SnapshotUpdateQueued?.Invoke(UpdatesMeta[commandPosition].Id);
        }
        else
        {
            // de-allocate previous buffer as a new one will be allocated
            MemoryStorage.Deallocate(UpdatesMeta[commandPosition].Index);

            //Debug.Log($"UPDATE to {networkVariable.Name} re-using existing command with ID #{UpdatesMeta[commandPosition].Id} -- NO #{command.NetworkObjectId} from tick {command.TickWritten}");

        }

        // the position we'll be serializing the network variable at, in our memory buffer
        var bufferPos = 0;

        m_Writer.Seek(0);
        m_Writer.Truncate(0);

        if (m_NumAvailableIndices == 0)
        {
            // todo: error handling
            Debug.Assert(false);
        }

        networkVariable.WriteDelta(m_Writer, clientId);
        command.SerializedLength = m_Writer.Length;

        var allocated = MemoryStorage.Allocate(UpdatesMeta[commandPosition].Index, m_Writer.Length, out bufferPos);

        Debug.Assert(allocated);

        unsafe
        {
            fixed (byte* buff = &MemoryBuffer[0])
            {
                Buffer.MemoryCopy(m_Writer.GetUnsafePtr(), buff + bufferPos, TotalBufferMemory - bufferPos, m_Writer.Length);
            }
        }

        Updates[commandPosition] = command;
        if (!UpdatesMeta[commandPosition].TargetClientIds.Contains(clientId))
        {
            UpdatesMeta[commandPosition].TargetClientIds.Add(clientId);
        }
        // KEEPSAKE FIX
        // since its possible (probable) that the data has changed (in case we're reusing an Update command slot) we clear any "first tick included" tracking
        // to resend this particular command to any not-yet-acked previous recipients, since they'll be acking the old data anyway
        UpdatesMeta[commandPosition].FirstTicksIncluded.Clear();
        foreach (var _ in UpdatesMeta[commandPosition].TargetClientIds)
        {
            UpdatesMeta[commandPosition].FirstTicksIncluded.Add(null);
        }
        // KEEPSAKE FIX END
        UpdatesMeta[commandPosition].BufferPos = bufferPos;

        // KEEPSAKE FIX - If the NetworkVariable has a higher snapshot group than the NetworkObject we use that.
        //                This is useful e.g. when the var refers to a NetworkObject part of this higher snapshot group and thus maybe not spawned on client yet.
        //                So that we don't send update commands referring to NetworkObjects we haven't told the client about yet.
        var snapshotGroup = networkVariable.m_NetworkBehaviour.NetworkObject.m_SnapshotGroup;
        if (networkVariable.SnapshotGroup.HasValue)
        {
            snapshotGroup = Math.Max(snapshotGroup, networkVariable.SnapshotGroup.Value);
        }
        UpdatesMeta[commandPosition].Group = Math.Max(UpdatesMeta[commandPosition].Group, snapshotGroup);

        //Debug.Log($"UPDATE #{UpdatesMeta[commandPosition].Id} for {networkVariable.Name} of {networkVariable.m_NetworkBehaviour} on NO #{networkVariable.m_NetworkBehaviour.NetworkObject.NetworkObjectId} is now from tick {command.TickWritten} with target client(s): {string.Join(", ", UpdatesMeta[commandPosition].TargetClientIds)}");
    }

    // KEEPSAKE FIX
    public byte GetCurrentSnapshotGroup(ulong clientId)
    {
        // Verify we allocated client Data for this clientId
        if (!m_ClientData.ContainsKey(clientId))
        {
            m_ClientData.Add(clientId, new ClientData(0));
        }

        var clientData = m_ClientData[clientId];
        return clientData.CurrentSnapshotGroup;
    }

    public void SetCurrentSnapshotGroup(ulong clientId, byte group)
    {
        // Verify we allocated client Data for this clientId
        if (!m_ClientData.ContainsKey(clientId))
        {
            m_ClientData.Add(clientId, new ClientData(0));
        }

        var clientData = m_ClientData[clientId];
        clientData.CurrentSnapshotGroup = group;
        m_ClientData[clientId] = clientData;
    }
    // END KEEPSAKE FIX

    internal void HandleSnapshot(ulong clientId, SnapshotDataMessage message)
    {
        // Read the Spawns. Count first, then each spawn
        var spawnCommand = new SnapshotSpawnCommand();
        var despawnCommand = new SnapshotDespawnCommand();
        var updateCommand = new UpdateCommand();

        var header = new SnapshotHeader();

        // Verify we allocated client Data for this clientId
        if (!m_ClientData.ContainsKey(clientId))
        {
            m_ClientData.Add(clientId, new ClientData(0));
        }

        if (message.ReadBuffer.TryBeginRead(FastBufferWriter.GetWriteSize(header)))
        {
            // todo: error handling
            message.ReadBuffer.ReadValue(out header);
        }

        var clientData = m_ClientData[clientId];
        clientData.LastReceivedTick = header.CurrentTick;

        /*if (header.SpawnCount > 0 || header.DespawnCount > 0 || header.UpdateCount > 0)
        {
            Debug.Log($"Handling snapshot {header.CurrentTick} from peer {clientId} (during our tick {m_CurrentTick}) containing S:{header.SpawnCount} D:{header.DespawnCount} U:{header.UpdateCount}, will ack next outgoing snapshot");
        }*/

        m_ClientData[clientId] = clientData;

        // KEEPSAKE FIX - since we're sending variable data with spawns, include that in the size calc ------------vvv
        var spawnsPayloadSize = FastBufferWriter.GetWriteSize(spawnCommand) * header.SpawnCount + header.SpawnVariableDataSize;
        if (!message.ReadBuffer.TryBeginRead(spawnsPayloadSize))
        {
            // todo: deal with error
            LogNetcode.Error($"Failed to read spawns payload of {spawnsPayloadSize} byte(s) from snapshot buffer ({header.SpawnCount} Spawn commands at {FastBufferWriter.GetWriteSize(spawnCommand)} a piece and a variable data payload of {header.SpawnVariableDataSize})");
            return;
        }

        for (var index = 0; index < header.SpawnCount; index++)
        {
            message.ReadBuffer.ReadValue(out spawnCommand);

            // KEEPSAKE FIX - support for including variable data in spawn command
            byte[] netVarBuffer = null;
            message.ReadBuffer.ReadValue(out int netVarSerializedLength);
            if (netVarSerializedLength != 0)
            {
                #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
                var markerPayloadLength = k_MarkerSpawnCommandNetVarBoundary.Length + FastBufferWriter.GetWriteSize<int>();
                netVarSerializedLength -= markerPayloadLength;
                #endif

                netVarBuffer = ArrayPool<byte>.Shared.Rent(netVarSerializedLength);
                message.ReadBuffer.ReadBytes(ref netVarBuffer, netVarSerializedLength);

                #if KEEPSAKE_BUILD_DEBUG || KEEPSAKE_BUILD_DEVELOPMENT
                message.ReadBuffer.ReadValue(out int markerLength);
                Assert.AreEqual(
                    k_MarkerSpawnCommandNetVarBoundary.Length,
                    markerLength,
                    $"Spawn command (NO #{spawnCommand.NetworkObjectId} / Hash {spawnCommand.GlobalObjectIdHash}) variable data boundary marker should be of expected length");
                for (var i = 0; i < markerLength; ++i)
                {
                    message.ReadBuffer.ReadValue(out byte actualByte);
                    Assert.AreEqual(
                        k_MarkerSpawnCommandNetVarBoundary[i],
                        actualByte,
                        $"Spawn command (NO #{spawnCommand.NetworkObjectId} / Hash {spawnCommand.GlobalObjectIdHash}) variable data boundary marker should be intact (byte {i})");
                }
                #endif
            }

            // KEEPSAKE FIX - use TryGetValue
            if (TickAppliedSpawn.TryGetValue(spawnCommand.NetworkObjectId, out var tickApplied) && spawnCommand.TickWritten <= tickApplied)
            {
                continue;
            }

            // KEEPSAKE FIX - if we received a spawn command and we're nested to an object that doesn't exist yet, stash this spawn command for later
            if (spawnCommand.RootNetworkObjectId > 0 && (!TickAppliedSpawn.ContainsKey(spawnCommand.RootNetworkObjectId) || !m_NetworkManager.SpawnManager.SpawnedObjects.ContainsKey(spawnCommand.RootNetworkObjectId)))
            {
                if (!m_PendingNestedSpawnCommands.TryGetValue(spawnCommand.RootNetworkObjectId, out var pendingNestedSpawnsForRoot))
                {
                    pendingNestedSpawnsForRoot = new List<PendingSpawnCommandData>();
                    m_PendingNestedSpawnCommands[spawnCommand.RootNetworkObjectId] = pendingNestedSpawnsForRoot;
                }

                pendingNestedSpawnsForRoot.Add(new PendingSpawnCommandData
                {
                    Command = spawnCommand,
                    NetVarBuffer = netVarBuffer,
                });
                continue;
            }
            // END KEEPSAKE FIX

            TickAppliedSpawn[spawnCommand.NetworkObjectId] = spawnCommand.TickWritten;
            // KEEPSAKE FIX - support for including variable data in spawn command
            SpawnObjectAsync(spawnCommand, clientId, netVarBuffer).Forget();
        }

        var updateCommandSize = FastBufferWriter.GetWriteSize(updateCommand);
        for (var index = 0; index < header.UpdateCount; index++)
        {
            message.ReadBuffer.TryBeginRead(updateCommandSize);
            message.ReadBuffer.ReadValue(out updateCommand);

            //NetworkVariableBase variable;
            GetBehaviourVariable(updateCommand, out var behaviour, out var variable, clientId);

            // KEEPSAKE FIX - we made SpawnObject above async, if we receive updates before spawn has completed stash them to be processed later
            if (behaviour == null)
            {
                // We might have received a snapshot update for a NetworkObject that is pending *Attach* via the old LegacyNetwork route, which we can't really check if its pending
                // todo: should these stashes expire? similar to Netcode's built in stashing of RPCs (see m_Triggers in NetworkSpawnManager)
                // For now stash all update commands that are for unknown objects, maybe the NO will come attaching some time soon and we can process these updates.
                /*if (!TickAppliedSpawn.TryGetValue(updateCommand.NetworkObjectId, out _))
                {
                    LogNetcode.Error($"Snapshot included update command for unknown network object #{updateCommand.NetworkObjectId}. Not spawned nor pending spawn!");
                    message.ReadBuffer.Seek(message.ReadBuffer.Position + updateCommand.SerializedLength);
                    continue;
                }*/

                if (!m_PendingUpdateCommands.TryGetValue(updateCommand.NetworkObjectId, out var pendingUpdates))
                {
                    pendingUpdates = new PendingUpdateCommandsData
                    {
                        Writer = new FastBufferWriter(2048, Allocator.Persistent, 1024 * 1024),
                        Headers = ListPool<PendingUpdateCommandHeader>.Get(),
                    };
                    m_PendingUpdateCommands[updateCommand.NetworkObjectId] = pendingUpdates;
                }
                else
                {
                    var commandAlreadyStashed = false;
                    foreach (var pendingUpdateHeader in pendingUpdates.Headers)
                    {
                        if (updateCommand.NetworkObjectId == pendingUpdateHeader.NetworkObjectId && updateCommand.BehaviourIndex == pendingUpdateHeader.BehaviourIndex && updateCommand.VariableIndex == pendingUpdateHeader.VariableIndex && updateCommand.TickWritten == pendingUpdateHeader.TickWritten && clientId == pendingUpdateHeader.ClientId)
                        {
                            commandAlreadyStashed = true;
                            break;
                        }
                    }

                    if (commandAlreadyStashed)
                    {
                        // seek past this update to continue iterating
                        message.ReadBuffer.Seek(message.ReadBuffer.Position + updateCommand.SerializedLength);
                        continue;
                    }
                }

                // tracking any pending update we have stashed to not stash the same command several times (will be sent several times until acked)
                pendingUpdates.Headers.Add(
                    new PendingUpdateCommandHeader
                    {
                        NetworkObjectId = updateCommand.NetworkObjectId,
                        BehaviourIndex = updateCommand.BehaviourIndex,
                        VariableIndex = updateCommand.VariableIndex,
                        TickWritten = updateCommand.TickWritten,
                        ClientId = clientId,
                    });
                var headerIndex = pendingUpdates.Headers.Count - 1;

                pendingUpdates.Writer.TryBeginWrite(sizeof(int) + updateCommandSize + updateCommand.SerializedLength);
                pendingUpdates.Writer.WriteValue(headerIndex);
                // write command "header"
                pendingUpdates.Writer.WriteValue(updateCommand);
                unsafe
                {
                    // write command "payload"
                    pendingUpdates.Writer.WriteBytes(message.ReadBuffer.GetUnsafePtr(), updateCommand.SerializedLength, message.ReadBuffer.Position);

                    // move the reader past the "payload" to continue iteration
                    message.ReadBuffer.Seek(message.ReadBuffer.Position + updateCommand.SerializedLength);
                }
                //LogNetcode.Debug($"Stashed pending update command for V#{updateCommand.VariableIndex} on B#{updateCommand.BehaviourIndex} on NO#{updateCommand.NetworkObjectId} for tick {updateCommand.TickWritten}, total stashed sized now {pendingUpdates.Writer.Length} byte(s). Header index is {headerIndex}");
                continue;
            }
            // END KEEPSAKE FIX

            // KEEPSAKE FIX - extracted to method to reuse
            ProcessUpdateCommand(clientId, message.ReadBuffer, updateCommand, variable, behaviour);
        }

        if (!message.ReadBuffer.TryBeginRead(FastBufferWriter.GetWriteSize(despawnCommand) * header.DespawnCount))
        {
            // todo: deal with error
        }

        for (var index = 0; index < header.DespawnCount; index++)
        {
            message.ReadBuffer.ReadValue(out despawnCommand);

            // todo: can we keep a single value of which tick we applied instead of per object ?
            if (TickAppliedDespawn.ContainsKey(despawnCommand.NetworkObjectId) && despawnCommand.TickWritten <= TickAppliedDespawn[despawnCommand.NetworkObjectId])
            {
                continue;
            }

            TickAppliedDespawn[despawnCommand.NetworkObjectId] = despawnCommand.TickWritten;
            DespawnObject(despawnCommand, clientId);
        }

        // todo: can we keep a single value of which tick we applied instead of per object ?

        for (var i = 0; i < NumSpawns;)
        {
            var clientIndex = SpawnsMeta[i].TargetClientIds.IndexOf(clientId);
            if (clientIndex >= 0)
            {
                var firstIncludedTick = SpawnsMeta[i].FirstTicksIncluded[clientIndex];
                if (firstIncludedTick.HasValue && firstIncludedTick.Value < header.LastReceivedSequence)
                {
                    SpawnsMeta[i].TargetClientIds.RemoveAt(clientIndex);
                    SpawnsMeta[i].FirstTicksIncluded.RemoveAt(clientIndex);
                    //Debug.Log($"SPAWN #{SpawnsMeta[i].Id} delivered to {clientId}. First included in {firstIncludedTick.Value} and the acked {header.LastReceivedSequence}");

                    if (SpawnsMeta[i].TargetClientIds.Count == 0)
                    {
                        // KEEPSAKE FIX - delivery tracking
                        //Debug.Log($"SPAWN #{SpawnsMeta[i].Id} DELIVERED TO ALL queued in tick {Spawns[i].TickWritten}. For last client {clientId} it was first included in {firstIncludedTick} which was acked {header.LastReceivedSequence}");
                        m_NetworkManager.SnapshotSpawnDelivered?.Invoke(SpawnsMeta[i].Id);

                        // KEEPSAKE FIX - using ListPool
                        ListPool<ulong>.Release(SpawnsMeta[i].TargetClientIds);
                        ListPool<int?>.Release(SpawnsMeta[i].FirstTicksIncluded);

                        // KEEPSAKE FIX - shift entire collection rather than moving tail to the now empty slot
                        //                this is to ensure sending and processing Spawns in the order they were queued
                        Array.Copy(SpawnsMeta, i + 1, SpawnsMeta, i, NumSpawns - 1 - i);
                        Array.Copy(Spawns, i + 1, Spawns, i, NumSpawns - 1 - i);
                        /*
                        SpawnsMeta[i] = SpawnsMeta[NumSpawns - 1];
                        Spawns[i] = Spawns[NumSpawns - 1];*/
                        NumSpawns--;

                        continue; // skip the i++ below
                    }
                }
            }
            i++;
        }

        for (var i = 0; i < NumDespawns;)
        {
            var clientIndex = DespawnsMeta[i].TargetClientIds.IndexOf(clientId);
            if (clientIndex >= 0)
            {
                var firstIncludedTick = DespawnsMeta[i].FirstTicksIncluded[clientIndex];
                if (firstIncludedTick.HasValue && firstIncludedTick.Value < header.LastReceivedSequence)
                {
                    DespawnsMeta[i].TargetClientIds.RemoveAt(clientIndex);
                    DespawnsMeta[i].FirstTicksIncluded.RemoveAt(clientIndex);
                    //Debug.Log($"DESPAWN #{DespawnsMeta[i].Id} delivered to {clientId}. First included in {firstIncludedTick.Value} and the acked {header.LastReceivedSequence}");

                    if (DespawnsMeta[i].TargetClientIds.Count == 0)
                    {
                        //Debug.Log($"DESPAWN #{DespawnsMeta[i].Id} DELIVERED TO ALL queued in tick {Despawns[i].TickWritten}. For last client {clientId} it was first included in {firstIncludedTick} which was acked {header.LastReceivedSequence}");

                        // KEEPSAKE FIX - delivery tracking
                        m_NetworkManager.SnapshotDespawnDelivered?.Invoke(DespawnsMeta[i].Id);

                        // KEEPSAKE FIX - using ListPool
                        ListPool<ulong>.Release(DespawnsMeta[i].TargetClientIds);
                        ListPool<int?>.Release(DespawnsMeta[i].FirstTicksIncluded);

                        // KEEPSAKE FIX - shift entire collection rather than moving tail to the now empty slot
                        //                this is to ensure sending and processing Despawns in the order they were queued
                        Array.Copy(DespawnsMeta, i + 1, DespawnsMeta, i, NumDespawns - 1 - i);
                        Array.Copy(Despawns, i + 1, Despawns, i, NumDespawns - 1 - i);
                        /*DespawnsMeta[i] = DespawnsMeta[NumDespawns - 1];
                        Despawns[i] = Despawns[NumDespawns - 1];*/
                        NumDespawns--;

                        continue; // skip the i++ below
                    }
                }
            }
            i++;
        }
        for (var i = 0; i < NumUpdates;)
        {
            var clientIndex = UpdatesMeta[i].TargetClientIds.IndexOf(clientId);
            if (clientIndex >= 0)
            {
                var firstIncludedTick = UpdatesMeta[i].FirstTicksIncluded[clientIndex];
                if (firstIncludedTick.HasValue && firstIncludedTick.Value < header.LastReceivedSequence)
                {
                    UpdatesMeta[i].TargetClientIds.RemoveAt(clientIndex);
                    UpdatesMeta[i].FirstTicksIncluded.RemoveAt(clientIndex);
                    //Debug.Log($"UPDATE #{UpdatesMeta[i].Id} delivered to {clientId}. First included in {firstIncludedTick.Value} and the acked {header.LastReceivedSequence}. Remaining target clients ({UpdatesMeta[i].TargetClientIds.Count}): {string.Join(", ", UpdatesMeta[i].TargetClientIds)}");

                    if (UpdatesMeta[i].TargetClientIds.Count == 0)
                    {
                        // KEEPSAKE FIX - delivery tracking
                        // no more outstanding clients, we've delivered :clap:
                        //Debug.Log($"UPDATE #{UpdatesMeta[i].Id} DELIVERED TO ALL queued in tick {Updates[i].TickWritten}. For last client {clientId} it was first included in {firstIncludedTick} which was acked {header.LastReceivedSequence}");
                        m_NetworkManager.SnapshotUpdateDelivered?.Invoke(UpdatesMeta[i].Id);

                        MemoryStorage.Deallocate(UpdatesMeta[i].Index);
                        m_AvailableIndices[m_NumAvailableIndices++] = UpdatesMeta[i].Index;

                        // KEEPSAKE FIX - using ListPool
                        ListPool<ulong>.Release(UpdatesMeta[i].TargetClientIds);
                        ListPool<int?>.Release(UpdatesMeta[i].FirstTicksIncluded);

                        // KEEPSAKE FIX - shift entire collection rather than moving tail to the now empty slot
                        //                this is to ensure sending and processing Updates in the order they were queued
                        Array.Copy(UpdatesMeta, i + 1, UpdatesMeta, i, NumUpdates - 1 - i);
                        Array.Copy(Updates, i + 1, Updates, i, NumUpdates - 1 - i);
                        /*UpdatesMeta[i] = UpdatesMeta[NumUpdates - 1];
                        Updates[i] = Updates[NumUpdates - 1];*/
                        NumUpdates--;

                        continue; // skip the i++ below
                    }
                }
            }
            i++;
        }

        //Debug.Log($"Done with handling snapshot {header.CurrentTick} from peer {clientId} (during our tick {m_CurrentTick})");
    }

    // KEEPSAKE FIX - extracted to method for reuse
    private void ProcessUpdateCommand(ulong clientId, FastBufferReader buffer, UpdateCommand updateCommand, NetworkVariableBase variable, NetworkBehaviour behaviour)
    {
        if (behaviour == null || variable == null || updateCommand.TickWritten <= variable.TickRead)
        {
            // skip over the value update payload we don't need to read
            buffer.Seek(buffer.Position + updateCommand.SerializedLength);
            return;
        }

        try
        {
            behaviour.IsInternalVariableWrite = true;

            //LogNetcode.Debug($"Processing Update command (from tick {updateCommand.TickWritten}) to {variable.Name} on {behaviour.GetType().Name} on NO #{behaviour.NetworkObjectId}");

            // KEEPSAKE FIX - ensure client is owner and has authority
            if (m_NetworkManager.IsServer && (!variable.OwnerHasAuthority || clientId != behaviour.OwnerClientId))
            {
                LogNetcode.Warning($"Client {clientId} sent snapshot to server trying to modify '{behaviour.GetType().Name}.{variable.Name}' on {behaviour.gameObject} which they don't have ownership or authority over. Sending real truth!");
                variable.SetDirty(true, clientId);
                buffer.Seek(buffer.Position + updateCommand.SerializedLength);
                return;
            }

            // KEEPSAKE FIX - ignore snapshot values when we have the authority
            if (variable.OwnerHasAuthority && behaviour.IsOwner)
            {
                buffer.Seek(buffer.Position + updateCommand.SerializedLength);
                return;
            }

            variable.TickRead = updateCommand.TickWritten;
            variable.ReadDelta(
                buffer,
                m_NetworkManager.IsServer); // KEEPSAKE FIX - pass keepDirtyDelta as true for servers (=applying snapshot from client) so we can tell other clients about it

            m_NetworkManager.NetworkMetrics.TrackNetworkVariableDeltaReceived(
                clientId,
                behaviour.NetworkObject,
                variable.Name,
                behaviour.__getTypeName(),
                20); // todo: what length ?
        }
        finally
        {
            behaviour.IsInternalVariableWrite = false;
        }
        }

    internal void NetworkGetBehaviourVariable(UpdateCommand updateCommand, out NetworkBehaviour behaviour, out NetworkVariableBase variable, ulong srcClientId)
    {
        // KEEPSAKE FIX - check Attached instead of Spawned
        if (m_NetworkManager.SpawnManager.AttachedObjects.TryGetValue(updateCommand.NetworkObjectId, out var networkObject))
        {
            behaviour = networkObject.GetNetworkBehaviourAtOrderIndex(updateCommand.BehaviourIndex);

            Debug.Assert(networkObject != null);

            if (updateCommand.VariableIndex >= behaviour.NetworkVariableFields.Count)
            {
                Debug.LogError($"Error when getting network behaviour variable during update command from tick {updateCommand.TickWritten} (during our tick {m_CurrentTick}). Behaviour {behaviour} (on NO #{updateCommand.NetworkObjectId}) has {behaviour.NetworkVariableFields.Count} net vars but we're looking for index {updateCommand.VariableIndex}");
                variable = null;
                behaviour = null;
                return;
            }

            variable = behaviour.NetworkVariableFields[updateCommand.VariableIndex];
        }
        else
        {
            variable = null;
            behaviour = null;
        }
    }

    internal int NetworkSendMessage(SnapshotDataMessage message, ulong clientId)
    {
        m_NetworkManager.SendMessage(ref message, NetworkDelivery.ReliableFragmentedSequenced, clientId);

        return 0;
    }

    // KEEPSAKE FIX
    private class PendingUpdateCommandsData
    {
        public List<PendingUpdateCommandHeader> Headers;
        public FastBufferWriter                 Writer;
    }

    private struct PendingUpdateCommandHeader
    {
        public ulong NetworkObjectId;
        public int   BehaviourIndex;
        public int   VariableIndex;
        public int   TickWritten;
        public ulong ClientId;
    }
}

}
