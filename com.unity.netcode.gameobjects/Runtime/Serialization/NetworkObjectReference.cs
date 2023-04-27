using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Cysharp.Threading.Tasks;
using Keepsake.Common;
using UnityEngine;

namespace Unity.Netcode
{

/// <summary>
///     A helper struct for serializing <see cref="NetworkObject" />s over the network. Can be used in RPCs and
///     <see cref="NetworkVariable{T}" />.
/// </summary>
public struct NetworkObjectReference : INetworkSerializable, IEquatable<NetworkObjectReference>
{
    private ulong m_NetworkObjectId;

    // KEEPSAKE FIX - support a "null" NetworkObjectReference
    public const ulong NullNetworkObjectId = 0;

    /// <summary>
    ///     The <see cref="NetworkObject.NetworkObjectId" /> of the referenced <see cref="NetworkObject" />.
    /// </summary>
    // KEEPSAKE FIX - made setter public so we can support NetworkObjectReferences over LegacyNetwork RPCs
    public ulong NetworkObjectId
    {
        get => m_NetworkObjectId;
        set => m_NetworkObjectId = value;
    }

    // KEEPSAKE FIX
    public static NetworkObjectReference Null => new() { m_NetworkObjectId = NullNetworkObjectId };

    /// <summary>
    ///     Creates a new instance of the <see cref="NetworkObjectReference" /> struct.
    /// </summary>
    /// <param name="networkObject">The <see cref="NetworkObject" /> to reference.</param>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="ArgumentException"></exception>
    public NetworkObjectReference(NetworkObject networkObject)
    {
        if (networkObject == null)
        {
            // KEEPSAKE FIX - support a "null" NetworkObjectReference
            m_NetworkObjectId = NullNetworkObjectId;
            return;
        }

        // KEEPSAKE FIX - check IsAttached and not IsSpawned
        if (networkObject.IsAttached == false)
        {
            throw new ArgumentException(
                $"{nameof(NetworkObjectReference)} can only be created from attached {nameof(NetworkObject)}s, not from '{networkObject.gameObject.Path(true)}', maybe it hasn't been attached to the network layer just yet.");
        }

        // KEEPSAKE FIX - support a "null" NetworkObjectReference
        if (networkObject.NetworkObjectId == NullNetworkObjectId)
        {
            // this should honestly never happen
            throw new ArgumentException(
                $"Actual NetworkObject with \"null\" ID {NullNetworkObjectId} encountered at {networkObject.gameObject.Path(true)}, this is not supported and it will replicate as \"null\"!");
        }

        m_NetworkObjectId = networkObject.NetworkObjectId;
    }

    /// <summary>
    ///     Creates a new instance of the <see cref="NetworkObjectReference" /> struct.
    /// </summary>
    /// <param name="gameObject">The GameObject from which the <see cref="NetworkObject" /> component will be referenced.</param>
    /// <exception cref="ArgumentNullException"></exception>
    /// <exception cref="ArgumentException"></exception>
    public NetworkObjectReference(GameObject gameObject)
    {
        if (gameObject == null)
        {
            // KEEPSAKE FIX - support a "null" NetworkObjectReference
            m_NetworkObjectId = NullNetworkObjectId;
            return;
        }

        var networkObject = gameObject.GetComponent<NetworkObject>();

        if (networkObject == null)
        {
            throw new ArgumentException(
                $"Cannot create {nameof(NetworkObjectReference)} from {nameof(GameObject)} '{gameObject.Path(true)}' without a {nameof(NetworkObject)} component.");
        }

        // KEEPSAKE FIX - check IsAttached and not IsSpawned
        if (networkObject.IsAttached == false)
        {
            throw new ArgumentException(
                $"{nameof(NetworkObjectReference)} can only be created from attached {nameof(NetworkObject)}s, not from '{networkObject.gameObject.Path(true)}', maybe it hasn't been attached to the network layer just yet.");
        }

        // KEEPSAKE FIX - support a "null" NetworkObjectReference
        if (networkObject.NetworkObjectId == NullNetworkObjectId)
        {
            // this should honestly never happen
            throw new ArgumentException(
                $"Actual NetworkObject with \"null\" ID {NullNetworkObjectId} encountered at {networkObject.gameObject.Path(true)}, this is not supported and it will replicate as \"null\"!");
        }

        m_NetworkObjectId = networkObject.NetworkObjectId;
    }

    public override string ToString()
    {
        return $"[{nameof(NetworkObjectReference)} {m_NetworkObjectId}]";
    }

    /// <summary>
    ///     Tries to get the <see cref="NetworkObject" /> referenced by this reference.
    /// </summary>
    /// <param name="networkObject">The <see cref="NetworkObject" /> which was found. Null if no object was found.</param>
    /// <param name="networkManager">The networkmanager. Uses <see cref="NetworkManager.Singleton" /> to resolve if null.</param>
    /// <returns>
    ///     True if the <see cref="NetworkObject" /> was found; False if the <see cref="NetworkObject" /> was not found.
    ///     This can happen if the <see cref="NetworkObject" /> has not been spawned yet. you can try getting the reference at
    ///     a later point in time.
    /// </returns>
    public bool TryGet(out NetworkObject networkObject, NetworkManager networkManager = null)
    {
        networkObject = Resolve(this, networkManager);
        return networkObject != null;
    }

    /// <summary>
    ///     Resolves the corresponding <see cref="NetworkObject" /> for this reference.
    /// </summary>
    /// <param name="networkObjectRef">The reference.</param>
    /// <param name="networkManager">The networkmanager. Uses <see cref="NetworkManager.Singleton" /> to resolve if null.</param>
    /// <returns>The resolves <see cref="NetworkObject" />. Returns null if the networkobject was not found</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static NetworkObject Resolve(NetworkObjectReference networkObjectRef, NetworkManager networkManager = null)
    {
        // KEEPSAKE FIX - support a "null" NetworkObjectReference
        if (networkObjectRef.m_NetworkObjectId == NullNetworkObjectId)
        {
            return null;
        }

        networkManager = networkManager != null ? networkManager : NetworkManager.Singleton;

        // KEEPSAKE FIX - seen when anyone tries to resolve from OnDestroy triggered by shutdown
        if (networkManager == null)
        {
            return null;
        }

        // KEEPSAKE FIX - find objects in Attached instead of Spawned collection
        networkManager.SpawnManager.AttachedObjects.TryGetValue(networkObjectRef.m_NetworkObjectId, out var networkObject);

        return networkObject;
    }

    /// <inheritdoc />
    public bool Equals(NetworkObjectReference other)
    {
        return m_NetworkObjectId == other.m_NetworkObjectId;
    }

    /// <inheritdoc />
    public override bool Equals(object obj)
    {
        return obj is NetworkObjectReference other && Equals(other);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return m_NetworkObjectId.GetHashCode();
    }

    /// <inheritdoc />
    public void NetworkSerialize<T>(BufferSerializer<T> serializer) where T : IReaderWriter
    {
        serializer.SerializeValue(ref m_NetworkObjectId);
    }

    public static implicit operator NetworkObject(NetworkObjectReference networkObjectRef)
    {
        return Resolve(networkObjectRef);
    }

    public static implicit operator NetworkObjectReference(NetworkObject networkObject)
    {
        return new NetworkObjectReference(networkObject);
    }

    public static implicit operator GameObject(NetworkObjectReference networkObjectRef)
    {
        return Resolve(networkObjectRef)?.gameObject;
    }

    public static implicit operator NetworkObjectReference(GameObject gameObject)
    {
        return new NetworkObjectReference(gameObject);
    }

    /// <summary>
    /// KEEPSAKE FIX
    /// Resolves the NetworkObjectReference to a GameObject, potentially waiting for it to exist.
    /// </summary>
    public async UniTask<GameObject> GetAsync(CancellationToken cancellationToken = default)
    {
        var @this = this;
        if (!@this.TryGet(out var resolved))
        {
            await UniTask.WaitWhile(() => @this.TryGet(out resolved) == false, cancellationToken: cancellationToken);
        }
        return resolved == null ? null : resolved.gameObject;
    }
    // END KEEPSAKE FIX
}

}
