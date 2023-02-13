using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Keepsake.Common;
using UnityEngine;

namespace Unity.Netcode
{

/// <summary>
///     KEEPSAKE FIX
///     A helper struct for serializing <see cref="Components" />s over the network, granted their gameObject also has a
///     NetworkObject component.
///     IMPORTANT: If the component has siblings of the same type then you must guarantee that the order is the
///     same on all connected peers, else the ComponentReference might resolve to a different sibling!
/// </summary>
public struct ComponentReference : INetworkSerializable, IEquatable<ComponentReference>
{
    private NetworkObjectReference m_NetworkObjectReference;
    private uint                   m_TypeHash;
    private byte                   m_Index;

    private static readonly Dictionary<uint, Type> k_TypeHashCache               = new();
    private static readonly List<Component>        k_PreAllocatedComponentBuffer = new();

    public bool IsNull => m_NetworkObjectReference.Equals(NetworkObjectReference.Null);

    public ulong NetworkObjectId => m_NetworkObjectReference.NetworkObjectId;

    [RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.SubsystemRegistration)]
    private static void InitializeStatic()
    {
        k_TypeHashCache.Clear();
    }

    /// <summary>
    ///     Creates a new instance of the <see cref="ComponentReference" /> struct.
    ///     IMPORTANT: If the provided component has siblings of the same type then you must guarantee that the order is the
    ///     same on all connected peers,
    ///     else the ComponentReference might resolve to a different sibling!
    /// </summary>
    /// <param name="component">The <see cref="Component" /> to reference.</param>
    /// <exception cref="ArgumentException"></exception>
    public ComponentReference(Component component)
    {
        if (component == null)
        {
            m_NetworkObjectReference = NetworkObjectReference.Null;
            m_TypeHash = default;
            m_Index = default;
            return;
        }

        var networkObject = component.gameObject.GetComponent<NetworkObject>();

        if (networkObject == null)
        {
            throw new ArgumentException(
                $"Cannot create {nameof(ComponentReference)} from {nameof(Component)} on a {nameof(GameObject)} without a {nameof(NetworkObject)} ({component.gameObject.Path(true)}).");
        }

        var componentType = component.GetType();
        var componentsOfSameType = component.gameObject.GetComponents(componentType);
        var index = (byte)0;
        for (; index < componentsOfSameType.Length; ++index)
        {
            if (componentsOfSameType[index] == component)
            {
                break;
            }
        }

        m_NetworkObjectReference = networkObject;
        m_TypeHash = CalculateTypeHash(componentType);
        m_Index = index;
    }

    /// <summary>
    ///     Tries to get the <see cref="Component" /> referenced by this reference.
    /// </summary>
    /// <param name="component">
    ///     The <see cref="Component" /> which was found. Null if the corresponding
    ///     <see cref="NetworkObject" /> was not found.
    /// </param>
    /// <param name="networkManager">The networkmanager. Uses <see cref="NetworkManager.Singleton" /> to resolve if null.</param>
    /// <returns>
    ///     True if the <see cref="Component" /> was found; False if the <see cref="Component" /> was not found. This can
    ///     happen if the corresponding <see cref="NetworkObject" /> has not been spawned yet. you can try getting the
    ///     reference at a later point in time.
    /// </returns>
    public bool TryGet(out Component component, NetworkManager networkManager = null)
    {
        component = GetInternal(this, null);
        return component != null;
    }

    /// <summary>
    ///     Tries to get the <see cref="Component" /> referenced by this reference.
    /// </summary>
    /// <param name="component">
    ///     The <see cref="Component" /> which was found. Null if the corresponding
    ///     <see cref="NetworkObject" /> was not found.
    /// </param>
    /// <param name="networkManager">The networkmanager. Uses <see cref="NetworkManager.Singleton" /> to resolve if null.</param>
    /// <typeparam name="T">The type of the component for convenience.</typeparam>
    /// <returns>
    ///     True if the <see cref="Component" /> was found; False if the <see cref="Component" /> was not found. This can
    ///     happen if the corresponding <see cref="NetworkObject" /> has not been spawned yet. you can try getting the
    ///     reference at a later point in time.
    /// </returns>
    public bool TryGet<T>(out T component, NetworkManager networkManager = null) where T : Component
    {
        component = (T)GetInternal(this, null);
        return component != null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Component GetInternal(ComponentReference componentRef, NetworkManager networkManager = null)
    {
        if (componentRef.m_NetworkObjectReference.TryGet(out var networkObject, networkManager)
            && TryResolveTypeHash(componentRef.m_TypeHash, out var componentType, networkObject))
        {
            try
            {
                networkObject.gameObject.GetComponents(componentType, k_PreAllocatedComponentBuffer);
                if (k_PreAllocatedComponentBuffer.Count > componentRef.m_Index)
                {
                    return k_PreAllocatedComponentBuffer[componentRef.m_Index];
                }
            }
            finally
            {
                k_PreAllocatedComponentBuffer.Clear();
            }
        }

        return null;
    }

    private static uint CalculateTypeHash(Type type)
    {
        return XXHash.Hash32(type.FullName);
    }

    private static bool TryResolveTypeHash(uint hash, out Type type, NetworkObject networkObject = null)
    {
        if (k_TypeHashCache.TryGetValue(hash, out type))
        {
            return true;
        }

        if (networkObject == null)
        {
            return false;
        }

        foreach (var c in networkObject.gameObject.GetComponents<Component>())
        {
            // we could check if already present in cache for each component but lets just recalculate all until Profiler tells us otherwise

            var t = c.GetType();
            var h = CalculateTypeHash(t);
            k_TypeHashCache[h] = t;

            if (h == hash)
            {
                type = t;
                return true;
            }
        }

        return false;
    }

    /// <inheritdoc />
    public bool Equals(ComponentReference other)
    {
        return m_NetworkObjectReference.Equals(other.m_NetworkObjectReference)
            && m_TypeHash == other.m_TypeHash
            && m_Index == other.m_Index;
    }

    /// <inheritdoc />
    public override bool Equals(object obj)
    {
        return obj is ComponentReference other && Equals(other);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        unchecked
        {
            var hashCode = m_NetworkObjectReference.GetHashCode();
            hashCode = (hashCode * 397) ^ m_TypeHash.GetHashCode();
            hashCode = (hashCode * 397) ^ m_Index.GetHashCode();
            return hashCode;
        }
    }

    /// <inheritdoc />
    public void NetworkSerialize<T>(BufferSerializer<T> serializer) where T : IReaderWriter
    {
        m_NetworkObjectReference.NetworkSerialize(serializer);
        serializer.SerializeValue(ref m_TypeHash);
        serializer.SerializeValue(ref m_Index);
    }

    public static implicit operator Component(ComponentReference componentRef)
    {
        return GetInternal(componentRef);
    }

    public static implicit operator ComponentReference(Component component)
    {
        return new ComponentReference(component);
    }
}

}
