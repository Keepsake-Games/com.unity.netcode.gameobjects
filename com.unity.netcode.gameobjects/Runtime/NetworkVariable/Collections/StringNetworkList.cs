using System;
using System.Collections.Generic;

namespace Unity.Netcode
{

/// <summary>
///     KEEPSAKE FIX - copy of NetworkList<> that instead of taking unmanaged type takes string
///     Event based NetworkVariable container for syncing Lists
/// </summary>
public class StringNetworkList : NetworkVariableBase
{
    /// <summary>
    ///     Delegate type for list changed event
    /// </summary>
    /// <param name="changeEvent">Struct containing information about the change event</param>
    public delegate void OnListChangedDelegate(NetworkListEvent<string> changeEvent);

    private readonly List<NetworkListEvent<string>> m_DirtyEvents = new(64);
    private readonly List<string>                   m_List        = new(64);

    private bool m_HasDirtyEvents;

    public int Count => m_List.Count;

    public string this[int index]
    {
        get => m_List[index];
        set
        {
            m_List[index] = value;

            var listEvent = new NetworkListEvent<string>()
            {
                Type = NetworkListEvent<string>.EventType.Value,
                Index = index,
                Value = value,
            };

            HandleAddListEvent(listEvent);
        }
    }

    public int LastModifiedTick =>
        // todo: implement proper network tick for NetworkList
        NetworkTickSystem.NoTick;

    /// <summary>
    ///     Creates a NetworkList with the default value and settings
    /// </summary>
    public StringNetworkList()
    {
    }

    /// <summary>
    ///     Creates a NetworkList with the default value and custom settings
    /// </summary>
    /// <param name="readPerm">The read permission to use for the NetworkList</param>
    /// <param name="values">The initial value to use for the NetworkList</param>
    public StringNetworkList(NetworkVariableReadPermission readPerm, IEnumerable<string> values) : base(readPerm)
    {
        foreach (var value in values)
        {
            m_List.Add(value);
        }
    }

    /// <summary>
    ///     Creates a NetworkList with a custom value and the default settings
    /// </summary>
    /// <param name="values">The initial value to use for the NetworkList</param>
    public StringNetworkList(IEnumerable<string> values)
    {
        foreach (var value in values)
        {
            m_List.Add(value);
        }
    }

    /// <summary>
    ///     The callback to be invoked when the list gets changed
    /// </summary>
    public event OnListChangedDelegate OnListChanged;

    /// <inheritdoc />
    public override void ResetDirty()
    {
        base.ResetDirty();
        m_DirtyEvents.Clear();
        m_HasDirtyEvents = false;
        m_IsDirtySubject.OnNext(IsDirty());
    }

    /// <inheritdoc />
    public override bool IsDirty()
    {
        // we call the base class to allow the SetDirty() mechanism to work
        return m_HasDirtyEvents || base.IsDirty();
    }

    /// <inheritdoc />
    public override void WriteDelta(FastBufferWriter writer)
    {
        if (base.IsDirty())
        {
            writer.WriteValueSafe((ushort)1);
            writer.WriteValueSafe(NetworkListEvent<string>.EventType.Full);
            WriteField(writer);

            return;
        }

        writer.WriteValueSafe((ushort)m_DirtyEvents.Count);
        for (var i = 0; i < m_DirtyEvents.Count; i++)
        {
            writer.WriteValueSafe(m_DirtyEvents[i].Type);
            switch (m_DirtyEvents[i].Type)
            {
                case NetworkListEvent<string>.EventType.Add:
                {
                    writer.WriteValueSafe(m_DirtyEvents[i].Value);
                }
                    break;
                case NetworkListEvent<string>.EventType.Insert:
                {
                    writer.WriteValueSafe(m_DirtyEvents[i].Index);
                    writer.WriteValueSafe(m_DirtyEvents[i].Value);
                }
                    break;
                case NetworkListEvent<string>.EventType.Remove:
                {
                    writer.WriteValueSafe(m_DirtyEvents[i].Value);
                }
                    break;
                case NetworkListEvent<string>.EventType.RemoveAt:
                {
                    writer.WriteValueSafe(m_DirtyEvents[i].Index);
                }
                    break;
                case NetworkListEvent<string>.EventType.Value:
                {
                    writer.WriteValueSafe(m_DirtyEvents[i].Index);
                    writer.WriteValueSafe(m_DirtyEvents[i].Value);
                }
                    break;
                case NetworkListEvent<string>.EventType.Clear:
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
        writer.WriteValueSafe((ushort)m_List.Count);
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
            reader.ReadValueSafe(out var value);
            m_List.Add(value);
        }

        // KEEPSAKE FIX - invoke changed event on full reads (why not Unity??)
        OnListChanged?.Invoke(
            new NetworkListEvent<string>
            {
                Type = NetworkListEvent<string>.EventType.Full,
            });
    }

    /// <inheritdoc />
    public override void ReadDelta(FastBufferReader reader, bool keepDirtyDelta)
    {
        reader.ReadValueSafe(out ushort deltaCount);
        for (var i = 0; i < deltaCount; i++)
        {
            reader.ReadValueSafe(out NetworkListEvent<string>.EventType eventType);
            switch (eventType)
            {
                case NetworkListEvent<string>.EventType.Add:
                {
                    reader.ReadValueSafe(out var value);
                    m_List.Add(value);

                    if (OnListChanged != null)
                    {
                        OnListChanged(
                            new NetworkListEvent<string>
                            {
                                Type = eventType,
                                Index = m_List.Count - 1,
                                Value = m_List[m_List.Count - 1],
                            });
                    }

                    if (keepDirtyDelta)
                    {
                        m_DirtyEvents.Add(
                            new NetworkListEvent<string>()
                            {
                                Type = eventType,
                                Index = m_List.Count - 1,
                                Value = m_List[m_List.Count - 1],
                            });
                        m_HasDirtyEvents = true;
                        m_IsDirtySubject.OnNext(IsDirty());
                    }
                }
                    break;
                case NetworkListEvent<string>.EventType.Insert:
                {
                    reader.ReadValueSafe(out int index);
                    reader.ReadValueSafe(out var value);
                    if (m_List.Capacity < index + 1)
                    {
                        m_List.Capacity = index + 1;
                    }

                    m_List[index] = value;

                    if (OnListChanged != null)
                    {
                        OnListChanged(
                            new NetworkListEvent<string>
                            {
                                Type = eventType,
                                Index = index,
                                Value = m_List[index],
                            });
                    }

                    if (keepDirtyDelta)
                    {
                        m_DirtyEvents.Add(
                            new NetworkListEvent<string>()
                            {
                                Type = eventType,
                                Index = index,
                                Value = m_List[index],
                            });
                        m_HasDirtyEvents = true;
                        m_IsDirtySubject.OnNext(IsDirty());
                    }
                }
                    break;
                case NetworkListEvent<string>.EventType.Remove:
                {
                    reader.ReadValueSafe(out var value);
                    var index = m_List.IndexOf(value);
                    if (index == -1)
                    {
                        break;
                    }

                    m_List.RemoveAt(index);

                    if (OnListChanged != null)
                    {
                        OnListChanged(
                            new NetworkListEvent<string>
                            {
                                Type = eventType,
                                Index = index,
                                Value = value,
                            });
                    }

                    if (keepDirtyDelta)
                    {
                        m_DirtyEvents.Add(
                            new NetworkListEvent<string>()
                            {
                                Type = eventType,
                                Index = index,
                                Value = value,
                            });
                        m_HasDirtyEvents = true;
                        m_IsDirtySubject.OnNext(IsDirty());
                    }
                }
                    break;
                case NetworkListEvent<string>.EventType.RemoveAt:
                {
                    reader.ReadValueSafe(out int index);
                    var value = m_List[index];
                    m_List.RemoveAt(index);

                    if (OnListChanged != null)
                    {
                        OnListChanged(
                            new NetworkListEvent<string>
                            {
                                Type = eventType,
                                Index = index,
                                Value = value,
                            });
                    }

                    if (keepDirtyDelta)
                    {
                        m_DirtyEvents.Add(
                            new NetworkListEvent<string>()
                            {
                                Type = eventType,
                                Index = index,
                                Value = value,
                            });
                        m_HasDirtyEvents = true;
                        m_IsDirtySubject.OnNext(IsDirty());
                    }
                }
                    break;
                case NetworkListEvent<string>.EventType.Value:
                {
                    reader.ReadValueSafe(out int index);
                    reader.ReadValueSafe(out var value);
                    if (index >= m_List.Count)
                    {
                        throw new Exception("Shouldn't be here, index is higher than list length");
                    }

                    var previousValue = m_List[index];
                    m_List[index] = value;

                    if (OnListChanged != null)
                    {
                        OnListChanged(
                            new NetworkListEvent<string>
                            {
                                Type = eventType,
                                Index = index,
                                Value = value,
                                PreviousValue = previousValue,
                            });
                    }

                    if (keepDirtyDelta)
                    {
                        m_DirtyEvents.Add(
                            new NetworkListEvent<string>()
                            {
                                Type = eventType,
                                Index = index,
                                Value = value,
                                PreviousValue = previousValue,
                            });
                        m_HasDirtyEvents = true;
                        m_IsDirtySubject.OnNext(IsDirty());
                    }
                }
                    break;
                case NetworkListEvent<string>.EventType.Clear:
                {
                    //Read nothing
                    m_List.Clear();

                    if (OnListChanged != null)
                    {
                        OnListChanged(
                            new NetworkListEvent<string>
                            {
                                Type = eventType,
                            });
                    }

                    if (keepDirtyDelta)
                    {
                        m_DirtyEvents.Add(
                            new NetworkListEvent<string>()
                            {
                                Type = eventType,
                            });
                        m_HasDirtyEvents = true;
                        m_IsDirtySubject.OnNext(IsDirty());
                    }
                }
                    break;
                case NetworkListEvent<string>.EventType.Full:
                {
                    ReadField(reader);
                    ResetDirty();
                }
                    break;
            }
        }
    }

    public IEnumerator<string> GetEnumerator()
    {
        return m_List.GetEnumerator();
    }

    public void Add(string item)
    {
        m_List.Add(item);

        var listEvent = new NetworkListEvent<string>()
        {
            Type = NetworkListEvent<string>.EventType.Add,
            Value = item,
            Index = m_List.Count - 1,
        };

        HandleAddListEvent(listEvent);
    }

    public void Clear()
    {
        m_List.Clear();

        var listEvent = new NetworkListEvent<string>()
        {
            Type = NetworkListEvent<string>.EventType.Clear,
        };

        HandleAddListEvent(listEvent);
    }

    public bool Contains(string item)
    {
        var index = m_List.IndexOf(item);
        return index != -1;
    }

    public bool Remove(string item)
    {
        var index = m_List.IndexOf(item);
        if (index == -1)
        {
            return false;
        }

        m_List.RemoveAt(index);
        var listEvent = new NetworkListEvent<string>()
        {
            Type = NetworkListEvent<string>.EventType.Remove,
            Value = item,
        };

        HandleAddListEvent(listEvent);
        return true;
    }

    public int IndexOf(string item)
    {
        return m_List.IndexOf(item);
    }

    public void Insert(int index, string item)
    {
        if (m_List.Capacity < index + 1)
        {
            m_List.Capacity = index + 1;
        }

        m_List[index] = item;

        var listEvent = new NetworkListEvent<string>()
        {
            Type = NetworkListEvent<string>.EventType.Insert,
            Index = index,
            Value = item,
        };

        HandleAddListEvent(listEvent);
    }

    public void RemoveAt(int index)
    {
        m_List.RemoveAt(index);

        var listEvent = new NetworkListEvent<string>()
        {
            Type = NetworkListEvent<string>.EventType.RemoveAt,
            Index = index,
        };

        HandleAddListEvent(listEvent);
    }

    private void HandleAddListEvent(NetworkListEvent<string> listEvent)
    {
        m_DirtyEvents.Add(listEvent);
        m_HasDirtyEvents = true;
        m_IsDirtySubject.OnNext(IsDirty());
        OnListChanged?.Invoke(listEvent);
    }

    public override void Dispose()
    {
        m_List.Clear();
        m_DirtyEvents.Clear();
        base.Dispose();
    }
}

}
