using System;
using UniRx;

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

        // KEEPSAKE FIX we want to know if net var is fully setup or not before certain validations kick in
        public bool IsInitialized { get; private set; }

        public void Initialize(NetworkBehaviour networkBehaviour)
        {
            m_NetworkBehaviour = networkBehaviour;
            IsInitialized = true;
        }

        protected NetworkVariableBase(NetworkVariableReadPermission readPermIn = NetworkVariableReadPermission.Everyone)
        {
            ReadPerm = readPermIn;
        }

        private protected bool m_IsDirty;

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
        public IObservable<bool> WhenDirty => m_IsDirtySubject.DistinctUntilChanged();

        protected readonly BehaviorSubject<bool> m_IsDirtySubject = new(false); // BehaviorSubject so that DistinctUntilChanged gets primed with current value
        // END KEEPSAKE FIX

        /// <summary>
        /// Sets whether or not the variable needs to be delta synced
        /// </summary>
        public virtual void SetDirty(bool isDirty)
        {
            m_IsDirty = isDirty;

            // KEEPSAKE FIX - observable IsDirty (important to use IsDirty(), not m_IsDirty, to respect subclass impl)
            m_IsDirtySubject.OnNext(IsDirty());
        }

        /// <summary>
        /// Resets the dirty state and marks the variable as synced / clean
        /// </summary>
        public virtual void ResetDirty()
        {
            m_IsDirty = false;

            // KEEPSAKE FIX - observable IsDirty (important to use IsDirty(), not m_IsDirty, to respect subclass impl)
            m_IsDirtySubject.OnNext(IsDirty());
        }

        /// <summary>
        /// Gets Whether or not the container is dirty
        /// </summary>
        /// <returns>Whether or not the container is dirty</returns>
        public virtual bool IsDirty()
        {
            return m_IsDirty;
        }

        public virtual bool ShouldWrite(ulong clientId, bool isServer)
        {
            return IsDirty() && isServer && CanClientRead(clientId);
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

        /// <summary>
        /// Writes the dirty changes, that is, the changes since the variable was last dirty, to the writer
        /// </summary>
        /// <param name="writer">The stream to write the dirty changes to</param>
        public abstract void WriteDelta(FastBufferWriter writer);

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
