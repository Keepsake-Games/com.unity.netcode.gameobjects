using System;
using System.Collections.Generic;
using UniRx;
using UniRx.Observers;
using Unity.Collections;

namespace Unity.Netcode
{
    /// <summary>
    /// Interface for network value containers
    /// </summary>
    public abstract class NetworkVariableBase : IDisposable
    {
        /// <summary>
        /// The delivery type (QoS) to send data with
        /// </summary>
        internal const NetworkDelivery Delivery = NetworkDelivery.ReliableFragmentedSequenced;

        // KEEPSAKE FIX made protected internal so we can access it within Netcode
        protected internal NetworkBehaviour m_NetworkBehaviour;

        // KEEPSAKE FIX in some cases the network variable can insist on its own snapshot group, eg if a NO with group 90 has
        //              a NetworkObjectReference var that gets assigned a NO with group 100 we don't want to send that update until group 100
        public byte? SnapshotGroup { get; set; }

        // KEEPSAKE FIX we want to know if net var is fully setup or not before certain validations kick in
        public bool IsInitialized { get; private set; }

        public IDisposable StartInitialize(NetworkBehaviour networkBehaviour)
        {
            m_NetworkBehaviour = networkBehaviour;

            return Disposable.Create(
                () =>
                {
                    IsInitialized = true;
                });
        }

        protected NetworkVariableBase(NetworkVariableReadPermission readPermIn = NetworkVariableReadPermission.Everyone)
        {
            ReadPerm = readPermIn;
        }

        // KEEPSAKE FIX - replaced with per-client IsDirty
        //private protected bool m_IsDirty;

        // KEEPSAKE FIX - per-client dirty flags
        private HashSet<ulong> m_IsDirtyForClient = new();
        private HashSet<ulong> m_IsDirtyForClientDouble = new();

        internal int TickRead = 0;

        /// <summary>
        /// Gets or sets the name of the network variable's instance
        /// (MemberInfo) where it was declared.
        /// </summary>
        // KEEPSAKE FIX - made public
        public string Name { get; set; }

        /// <summary>
        /// The read permission for this var
        /// </summary>
        public readonly NetworkVariableReadPermission ReadPerm;

        // KEEPSAKE FIX
        /// <summary>
        /// When true the server will accept modifications of netvar as part of snapshots from owning client
        /// </summary>
        public bool OwnerHasAuthority;

        // KEEPSAKE FIX
        /// <summary>
        /// The amount of seconds that must pass between this variable being included in snapshots, no matter if dirty or not.
        /// </summary>
        public float MinSendDelay;

        // KEEPSAKE FIX
        /// <summary>
        /// Don't write until this tick, even if dirty. Based on MinSendDelay above and the prev tick it was written.
        /// </summary>
        public int MinNextTickWrite;

        // KEEPSAKE FIX - observable IsDirty so that NetworkBehaviour doesn't have to poll
        public IObservable<(bool, ulong)> WhenDirtyForClient => m_DirtyForClientSubject;

        protected readonly Subject<(bool, ulong)> m_DirtyForClientSubject = new Subject<(bool, ulong)>();
        // END KEEPSAKE FIX

        /* KEEPSAKE FIX - replaced with per-client IsDirty
        /// <summary>
        /// Sets whether or not the variable needs to be delta synced
        /// </summary>
        public virtual void SetDirty(bool isDirty)
        {
            m_IsDirty = isDirty;

            // KEEPSAKE FIX - observable IsDirty (important to use IsDirty(), not m_IsDirty, to respect subclass impl)
            m_IsDirtySubject.OnNext(IsDirty());
        }*/

        /// <summary>
        /// KEEPSAKE FIX - added per-client dirty flag
        /// </summary>
        public void SetDirty(bool isDirty, ulong clientId)
        {
            if (isDirty)
            {
                if (m_IsDirtyForClient.Add(clientId))
                {
                    m_DirtyForClientSubject.OnNext((true, clientId));
                }
            }
            else
            {
                if (m_IsDirtyForClient.Remove(clientId))
                {
                    m_DirtyForClientSubject.OnNext((false, clientId));
                }
            }
        }

        public void SetDirtyForAll(bool isDirty)
        {
            if (!m_NetworkBehaviour.NetworkManager.IsServer)
            {
                SetDirty(isDirty, m_NetworkBehaviour.NetworkManager.ServerClientId);
            }
            else
            {
                foreach (var clientId in m_NetworkBehaviour.NetworkManager.ConnectedClientsIds)
                {
                    if (clientId != m_NetworkBehaviour.NetworkManager.ServerClientId)
                    {
                        SetDirty(isDirty, clientId);
                    }
                }
            }
        }

        /// <summary>
        /// Resets the dirty state and marks the variable as synced / clean
        /// </summary>
        public virtual void ResetDirty(ulong? clientId)
        {
            // KEEPSAKE FIX - per client dirty flag
            if (clientId.HasValue)
            {
                if (m_IsDirtyForClient.Remove(clientId.Value))
                {
                    // important to use IsDirty(), not m_IsDirty, to respect subclass impl
                    m_DirtyForClientSubject.OnNext((IsDirty(clientId.Value), clientId.Value));
                }
            }
            else
            {
                (m_IsDirtyForClient, m_IsDirtyForClientDouble) = (m_IsDirtyForClientDouble, m_IsDirtyForClient);
                m_IsDirtyForClient.Clear();

                foreach (var c in m_IsDirtyForClientDouble)
                {
                    // important to use IsDirty(), not m_IsDirty, to respect subclass impl
                    m_DirtyForClientSubject.OnNext((IsDirty(c), c));
                }

                m_IsDirtyForClientDouble.Clear();
            };
        }

        /* KEEPSAKE FIX - replaced with per-client IsDirty
        /// <summary>
        /// Gets Whether or not the container is dirty
        /// </summary>
        /// <returns>Whether or not the container is dirty</returns>
        public virtual bool IsDirty()
        {
            return m_IsDirty;
        }*/

        /// <summary>
        /// KEEPSAKE FIX - added per-client dirty flags
        /// </summary>
        public virtual bool IsDirty(ulong? clientId)
        {
            if (clientId.HasValue)
            {
                return m_IsDirtyForClient.Contains(clientId.Value);
            }

            return m_IsDirtyForClient.Count > 0;
        }

        protected bool IsBaseDirty()
        {
            return m_IsDirtyForClient.Count > 0;
        }

        public IEnumerable<ulong> DirtyForClients()
        {
            return m_IsDirtyForClient;
        }

        public virtual bool ShouldWrite(ulong clientId, bool isServer)
        {
            return IsDirty(clientId) && isServer && CanClientRead(clientId);
        }

        /// <summary>
        /// Gets Whether or not a specific client can read to the varaible
        /// </summary>
        /// <param name="clientId">The clientId of the remote client</param>
        /// <returns>Whether or not the client can read to the variable</returns>
        public bool CanClientRead(ulong clientId)
        {
            switch (ReadPerm)
            {
                case NetworkVariableReadPermission.Everyone:
                    return true;
                case NetworkVariableReadPermission.OwnerOnly:
                    return m_NetworkBehaviour.OwnerClientId == clientId;
            }
            return true;
        }


        // KEEPSAKE FIX - we added net var-level snapshot group, see comment on SnapshotGroup field above
        public bool IsClientInSnapshotGroup(ulong clientId)
        {
            if (!SnapshotGroup.HasValue)
            {
                return true;
            }

            var clientGroup = m_NetworkBehaviour.NetworkManager.SnapshotSystem.GetCurrentSnapshotGroup(clientId);
            return clientGroup >= SnapshotGroup.Value;
        }

        /// <summary>
        /// Writes the dirty changes, that is, the changes since the variable was last dirty, to the writer
        /// </summary>
        /// <param name="writer">The stream to write the dirty changes to</param>
        /// <param name="clientId">KEEPSAKE FIX - since we added per-client dirty, support for writing different deltas to clients</param>
        public abstract void WriteDelta(FastBufferWriter writer, ulong clientId);

        /// <summary>
        /// Writes the complete state of the variable to the writer
        /// </summary>
        /// <param name="writer">The stream to write the state to</param>
        public abstract void WriteField(FastBufferWriter writer);

        /// <summary>
        /// Reads the complete state from the reader and applies it
        /// </summary>
        /// <param name="reader">The stream to read the state from</param>
        public abstract void ReadField(FastBufferReader reader);

        /// <summary>
        /// Reads delta from the reader and applies them to the internal value
        /// </summary>
        /// <param name="reader">The stream to read the delta from</param>
        /// <param name="keepDirtyDelta">Whether or not the delta should be kept as dirty or consumed</param>

        public abstract void ReadDelta(FastBufferReader reader, bool keepDirtyDelta);

        public virtual void Dispose()
        {
            IsInitialized = false;
        }
    }
}
