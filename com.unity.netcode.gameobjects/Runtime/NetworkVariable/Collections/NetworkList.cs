using System;
using System.Collections.Generic;
using Unity.Collections;

namespace Unity.Netcode
{
    /// <summary>
    /// Event based NetworkVariable container for syncing Lists
    /// </summary>
    /// <typeparam name="T">The type for the list</typeparam>
    public class NetworkList<T> : NetworkVariableBase where T : unmanaged, IEquatable<T>
    {
        private NativeList<T> m_List = new NativeList<T>(64, Allocator.Persistent);

        // KEEPSAKE FIX per-client dirty flags
        //private NativeList<NetworkListEvent<T>> m_DirtyEvents = new NativeList<NetworkListEvent<T>>(64, Allocator.Persistent);
        private readonly Dictionary<ulong, NativeList<NetworkListEvent<T>>> m_DirtyEvents = new();

        /// <summary>
        /// Delegate type for list changed event
        /// </summary>
        /// <param name="changeEvent">Struct containing information about the change event</param>
        public delegate void OnListChangedDelegate(NetworkListEvent<T> changeEvent);

        /// <summary>
        /// The callback to be invoked when the list gets changed
        /// </summary>
        public event OnListChangedDelegate OnListChanged;

        // KEEPSAKE FIX - dedicated bool to avoid checking Length in hot path
        private NativeParallelHashMap<ulong, bool> m_HasDirtyEvents = new(20, Allocator.Persistent);

        /// <summary>
        /// Creates a NetworkList with the default value and settings
        /// </summary>
        public NetworkList() { }

        /// <summary>
        /// Creates a NetworkList with the default value and custom settings
        /// </summary>
        /// <param name="readPerm">The read permission to use for the NetworkList</param>
        /// <param name="values">The initial value to use for the NetworkList</param>
        public NetworkList(NetworkVariableReadPermission readPerm, IEnumerable<T> values) : base(readPerm)
        {
            foreach (var value in values)
            {
                m_List.Add(value);
            }
        }

        /// <summary>
        /// Creates a NetworkList with a custom value and the default settings
        /// </summary>
        /// <param name="values">The initial value to use for the NetworkList</param>
        public NetworkList(IEnumerable<T> values)
        {
            foreach (var value in values)
            {
                m_List.Add(value);

            }
        }

        /// <inheritdoc />
        public override void ResetDirty(ulong? clientId)
        {
            base.ResetDirty(clientId);

            if (clientId.HasValue)
            {
                if (m_DirtyEvents.TryGetValue(clientId.Value, out var dirtyEvents))
                {
                    dirtyEvents.Clear();
                }

                m_HasDirtyEvents[clientId.Value] = false;
            }
            else
            {
                foreach (var kvp in m_DirtyEvents)
                {
                    kvp.Value.Clear();
                }

                m_HasDirtyEvents.Clear();
            }
        }

        /// <inheritdoc />
        public override bool IsDirty(ulong? clientId)
        {
            if (clientId.HasValue)
            {
                return (m_HasDirtyEvents.TryGetValue(clientId.Value, out var hasDirtyEvents) && hasDirtyEvents) || base.IsDirty(clientId);
            }

            // clientId == null => is dirty for any client?
            foreach (var kvp in m_HasDirtyEvents)
            {
                if (kvp.Value)
                {
                    return true;
                }
            }

            // we call the base class to allow the SetDirty() mechanism to work
            return base.IsDirty(clientId);
        }

        /// <inheritdoc />
        public override void WriteDelta(FastBufferWriter writer, ulong clientId)
        {
            if (IsBaseDirty())
            {
                writer.WriteValueSafe((ushort)1);
                writer.WriteValueSafe(NetworkListEvent<T>.EventType.Full);
                WriteField(writer);

                return;
            }

            if (!m_DirtyEvents.TryGetValue(clientId, out var dirtyEvents))
            {
                writer.WriteValueSafe((ushort)0);
                return;
            }

            writer.WriteValueSafe((ushort)dirtyEvents.Length);
            for (int i = 0; i < dirtyEvents.Length; i++)
            {
                writer.WriteValueSafe(dirtyEvents[i].Type);
                switch (dirtyEvents[i].Type)
                {
                    case NetworkListEvent<T>.EventType.Add:
                        {
                            writer.WriteValueSafe(dirtyEvents[i].Value);
                        }
                        break;
                    case NetworkListEvent<T>.EventType.Insert:
                        {
                            writer.WriteValueSafe(dirtyEvents[i].Index);
                            writer.WriteValueSafe(dirtyEvents[i].Value);
                        }
                        break;
                    case NetworkListEvent<T>.EventType.Remove:
                        {
                            writer.WriteValueSafe(dirtyEvents[i].Value);
                        }
                        break;
                    case NetworkListEvent<T>.EventType.RemoveAt:
                        {
                            writer.WriteValueSafe(dirtyEvents[i].Index);
                        }
                        break;
                    case NetworkListEvent<T>.EventType.Value:
                        {
                            writer.WriteValueSafe(dirtyEvents[i].Index);
                            writer.WriteValueSafe(dirtyEvents[i].Value);
                        }
                        break;
                    case NetworkListEvent<T>.EventType.Clear:
                        {
                            //Nothing has to be written
                        }
                        break;
                }
            }
        }

        /// <inheritdoc />
        public override void WriteField(FastBufferWriter writer)
        {
            writer.WriteValueSafe((ushort)m_List.Length);
            foreach (var e in m_List)
            {
                writer.WriteValueSafe(e);
            }
        }

        /// <inheritdoc />
        public override void ReadField(FastBufferReader reader)
        {
            m_List.Clear();
            reader.ReadValueSafe(out ushort count);
            for (var i = 0; i < count; i++)
            {
                reader.ReadValueSafe(out T value);
                m_List.Add(value);
            }

            // KEEPSAKE FIX - invoke changed event on full reads (why not Unity??)
            OnListChanged?.Invoke(new NetworkListEvent<T>
            {
                Type = NetworkListEvent<T>.EventType.Full,
            });
        }

        /// <inheritdoc />
        public override void ReadDelta(FastBufferReader reader, bool keepDirtyDelta)
        {
            reader.ReadValueSafe(out ushort deltaCount);
            for (int i = 0; i < deltaCount; i++)
            {
                reader.ReadValueSafe(out NetworkListEvent<T>.EventType eventType);
                switch (eventType)
                {
                    case NetworkListEvent<T>.EventType.Add:
                        {
                            reader.ReadValueSafe(out T value);
                            m_List.Add(value);

                            if (OnListChanged != null)
                            {
                                OnListChanged(new NetworkListEvent<T>
                                {
                                    Type = eventType,
                                    Index = m_List.Length - 1,
                                    Value = m_List[m_List.Length - 1]
                                });
                            }

                            if (keepDirtyDelta)
                            {
                                AddDirtyEventForAll(new NetworkListEvent<T>()
                                {
                                    Type = eventType,
                                    Index = m_List.Length - 1,
                                    Value = m_List[m_List.Length - 1]
                                });
                            }
                        }
                        break;
                    case NetworkListEvent<T>.EventType.Insert:
                        {
                            reader.ReadValueSafe(out int index);
                            reader.ReadValueSafe(out T value);
                            m_List.InsertRangeWithBeginEnd(index, index + 1);
                            m_List[index] = value;

                            if (OnListChanged != null)
                            {
                                OnListChanged(new NetworkListEvent<T>
                                {
                                    Type = eventType,
                                    Index = index,
                                    Value = m_List[index]
                                });
                            }

                            if (keepDirtyDelta)
                            {
                                AddDirtyEventForAll(new NetworkListEvent<T>()
                                {
                                    Type = eventType,
                                    Index = index,
                                    Value = m_List[index]
                                });
                            }
                        }
                        break;
                    case NetworkListEvent<T>.EventType.Remove:
                        {
                            reader.ReadValueSafe(out T value);
                            int index = m_List.IndexOf(value);
                            if (index == -1)
                            {
                                break;
                            }

                            m_List.RemoveAt(index);

                            if (OnListChanged != null)
                            {
                                OnListChanged(new NetworkListEvent<T>
                                {
                                    Type = eventType,
                                    Index = index,
                                    Value = value
                                });
                            }

                            if (keepDirtyDelta)
                            {
                                AddDirtyEventForAll(new NetworkListEvent<T>()
                                {
                                    Type = eventType,
                                    Index = index,
                                    Value = value
                                });
                            }
                        }
                        break;
                    case NetworkListEvent<T>.EventType.RemoveAt:
                        {
                            reader.ReadValueSafe(out int index);
                            T value = m_List[index];
                            m_List.RemoveAt(index);

                            if (OnListChanged != null)
                            {
                                OnListChanged(new NetworkListEvent<T>
                                {
                                    Type = eventType,
                                    Index = index,
                                    Value = value
                                });
                            }

                            if (keepDirtyDelta)
                            {
                                AddDirtyEventForAll(new NetworkListEvent<T>()
                                {
                                    Type = eventType,
                                    Index = index,
                                    Value = value
                                });
                            }
                        }
                        break;
                    case NetworkListEvent<T>.EventType.Value:
                        {
                            reader.ReadValueSafe(out int index);
                            reader.ReadValueSafe(out T value);
                            if (index >= m_List.Length)
                            {
                                throw new Exception("Shouldn't be here, index is higher than list length");
                            }

                            var previousValue = m_List[index];
                            m_List[index] = value;

                            if (OnListChanged != null)
                            {
                                OnListChanged(new NetworkListEvent<T>
                                {
                                    Type = eventType,
                                    Index = index,
                                    Value = value,
                                    PreviousValue = previousValue
                                });
                            }

                            if (keepDirtyDelta)
                            {
                                AddDirtyEventForAll(new NetworkListEvent<T>()
                                {
                                    Type = eventType,
                                    Index = index,
                                    Value = value,
                                    PreviousValue = previousValue
                                });
                            }
                        }
                        break;
                    case NetworkListEvent<T>.EventType.Clear:
                        {
                            //Read nothing
                            m_List.Clear();

                            if (OnListChanged != null)
                            {
                                OnListChanged(new NetworkListEvent<T>
                                {
                                    Type = eventType,
                                });
                            }

                            if (keepDirtyDelta)
                            {
                                AddDirtyEventForAll(new NetworkListEvent<T>()
                                {
                                    Type = eventType
                                });
                            }
                        }
                        break;
                    case NetworkListEvent<T>.EventType.Full:
                        {
                            ReadField(reader);
                            ResetDirty(null);
                        }
                        break;
                }
            }
        }

        /// <inheritdoc />
        public IEnumerator<T> GetEnumerator()
        {
            return m_List.GetEnumerator();
        }

        /// <inheritdoc />
        public void Add(T item)
        {
            m_List.Add(item);

            var listEvent = new NetworkListEvent<T>()
            {
                Type = NetworkListEvent<T>.EventType.Add,
                Value = item,
                Index = m_List.Length - 1
            };

            HandleAddListEvent(listEvent);
        }

        /// <inheritdoc />
        public void Clear()
        {
            m_List.Clear();

            var listEvent = new NetworkListEvent<T>()
            {
                Type = NetworkListEvent<T>.EventType.Clear
            };

            HandleAddListEvent(listEvent);
        }

        /// <inheritdoc />
        public bool Contains(T item)
        {
            int index = NativeArrayExtensions.IndexOf(m_List, item);
            return index != -1;
        }

        /// <inheritdoc />
        public bool Remove(T item)
        {
            int index = NativeArrayExtensions.IndexOf(m_List, item);
            if (index == -1)
            {
                return false;
            }

            m_List.RemoveAt(index);
            var listEvent = new NetworkListEvent<T>()
            {
                Type = NetworkListEvent<T>.EventType.Remove,
                Value = item
            };

            HandleAddListEvent(listEvent);
            return true;
        }

        /// <inheritdoc />
        public int Count => m_List.Length;

        /// <inheritdoc />
        public int IndexOf(T item)
        {
            return m_List.IndexOf(item);
        }

        /// <inheritdoc />
        public void Insert(int index, T item)
        {
            m_List.InsertRangeWithBeginEnd(index, index + 1);
            m_List[index] = item;

            var listEvent = new NetworkListEvent<T>()
            {
                Type = NetworkListEvent<T>.EventType.Insert,
                Index = index,
                Value = item
            };

            HandleAddListEvent(listEvent);
        }

        /// <inheritdoc />
        public void RemoveAt(int index)
        {
            m_List.RemoveAt(index);

            var listEvent = new NetworkListEvent<T>()
            {
                Type = NetworkListEvent<T>.EventType.RemoveAt,
                Index = index
            };

            HandleAddListEvent(listEvent);
        }

        /// <inheritdoc />
        public T this[int index]
        {
            get => m_List[index];
            set
            {
                m_List[index] = value;

                var listEvent = new NetworkListEvent<T>()
                {
                    Type = NetworkListEvent<T>.EventType.Value,
                    Index = index,
                    Value = value
                };

                HandleAddListEvent(listEvent);
            }
        }

        private void HandleAddListEvent(NetworkListEvent<T> listEvent)
        {
            AddDirtyEventForAll(listEvent);

            OnListChanged?.Invoke(listEvent);
        }

        private void AddDirtyEventForAll(NetworkListEvent<T> listEvent)
        {
            if (!m_NetworkBehaviour.NetworkManager.IsServer)
            {
                AddDirtyEvent(listEvent, m_NetworkBehaviour.NetworkManager.ServerClientId);
            }
            else
            {
                foreach (var clientId in m_NetworkBehaviour.NetworkManager.ConnectedClientsIds)
                {
                    if (clientId != m_NetworkBehaviour.NetworkManager.ServerClientId)
                    {
                        AddDirtyEvent(listEvent, clientId);
                    }
                }
            }
        }

        private void AddDirtyEvent(NetworkListEvent<T> listEvent, ulong clientId)
        {
            // NOTE: this will not call OnListChanged, you probably want to use HandleAddListEvent

            if (!m_DirtyEvents.TryGetValue(clientId, out var dirtyEvents))
            {
                dirtyEvents = new NativeList<NetworkListEvent<T>>(Allocator.Persistent);
                m_DirtyEvents[clientId] = dirtyEvents;
            }

            dirtyEvents.Add(listEvent);
            m_HasDirtyEvents[clientId] = true;
            m_DirtyForClientSubject.OnNext((true, clientId));
        }

        public int LastModifiedTick
        {
            get
            {
                // todo: implement proper network tick for NetworkList
                return NetworkTickSystem.NoTick;
            }
        }

        public override void Dispose()
        {
            if (m_List.IsCreated)
            {
                m_List.Dispose();
            }

            foreach (var kvp in m_DirtyEvents)
            {
                kvp.Value.Dispose();
            }

            m_DirtyEvents.Clear();

            m_HasDirtyEvents.Dispose();

            base.Dispose();
        }
    }

    /// <summary>
    /// Struct containing event information about changes to a NetworkList.
    /// </summary>
    /// <typeparam name="T">The type for the list that the event is about</typeparam>
    public struct NetworkListEvent<T>
    {
        /// <summary>
        /// Enum representing the different operations available for triggering an event.
        /// </summary>
        public enum EventType : byte
        {
            /// <summary>
            /// Add
            /// </summary>
            Add,

            /// <summary>
            /// Insert
            /// </summary>
            Insert,

            /// <summary>
            /// Remove
            /// </summary>
            Remove,

            /// <summary>
            /// Remove at
            /// </summary>
            RemoveAt,

            /// <summary>
            /// Value changed
            /// </summary>
            Value,

            /// <summary>
            /// Clear
            /// </summary>
            Clear,

            /// <summary>
            /// Full list refresh
            /// </summary>
            Full
        }

        /// <summary>
        /// Enum representing the operation made to the list.
        /// </summary>
        public EventType Type;

        /// <summary>
        /// The value changed, added or removed if available.
        /// </summary>
        public T Value;

        /// <summary>
        /// The previous value when "Value" has changed, if available.
        /// </summary>
        public T PreviousValue;

        /// <summary>
        /// the index changed, added or removed if available
        /// </summary>
        public int Index;
    }
}
