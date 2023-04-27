using Unity.Collections;
using UnityEngine;

namespace Unity.Netcode
{

internal struct SnapshotDataMessage : INetworkMessage
{
    internal         FastBufferWriter WriteBuffer;
    internal         FastBufferReader ReadBuffer;

    // a constructor with an unused parameter is used because C# doesn't allow parameter-less constructors
    public SnapshotDataMessage(int unused)
    {
        // KEEPSAKE FIX - changes to buffer size, allowing snapshots to grow a bit larger (since we include full net var state in spawn commands)
        const int initialBufferSize = 1024 * 1024;
        const int maxBufferSize = 10 * 1024 * 1024;

        WriteBuffer = new FastBufferWriter(initialBufferSize, Allocator.Temp, maxBufferSize);

        // KEEPSAKE FIX - why copy data from WriteBuffer to ReadBuffer when it isn't used?
        //ReadBuffer = new FastBufferReader(WriteBuffer, Allocator.Temp);
        ReadBuffer = default;
    }

    public void Serialize(FastBufferWriter writer)
    {
        if (!writer.TryBeginWrite(WriteBuffer.Length))
        {
            Debug.Log("Serialize. Not enough buffer");
        }
        writer.CopyFrom(WriteBuffer);
    }

    public bool Deserialize(FastBufferReader reader, ref NetworkContext context)
    {
        ReadBuffer = reader;
        return true;
    }

    public void Handle(ref NetworkContext context)
    {
        var systemOwner = context.SystemOwner;
        var senderId = context.SenderId;
        if (systemOwner is NetworkManager)
        {
            var networkManager = (NetworkManager)systemOwner;

            // todo: temporary hack around bug
            if (!networkManager.IsServer)
            {
                senderId = networkManager.ServerClientId;
            }

            var snapshotSystem = networkManager.SnapshotSystem;
            snapshotSystem.HandleSnapshot(senderId, this);
        }
    }
}

}
