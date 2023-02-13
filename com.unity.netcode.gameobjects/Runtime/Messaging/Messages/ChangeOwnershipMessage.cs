namespace Unity.Netcode
{
    internal struct ChangeOwnershipMessage : INetworkMessage
    {
        public ulong NetworkObjectId;
        public ulong OwnerClientId;

        public void Serialize(FastBufferWriter writer)
        {
            writer.WriteValueSafe(this);
        }
        public bool Deserialize(FastBufferReader reader, ref NetworkContext context)
        {
            var networkManager = (NetworkManager)context.SystemOwner;
            if (!networkManager.IsClient)
            {
                return false;
            }
            reader.ReadValueSafe(out this);
            // KEEPSAKE FIX - check in Attached instead of Spawned, feels a bit weird with triggering the OnSpawn if not but I guess Netcode knows?
            if (!networkManager.SpawnManager.AttachedObjects.ContainsKey(NetworkObjectId))
            {
                networkManager.SpawnManager.TriggerOnSpawn(NetworkObjectId, reader, ref context);
                return false;
            }

            return true;
        }

        public void Handle(ref NetworkContext context)
        {

            var networkManager = (NetworkManager)context.SystemOwner;
            // KEEPSAKE FIX - Attached instead of Spawned
            var networkObject = networkManager.SpawnManager.AttachedObjects[NetworkObjectId];

            if (networkObject.OwnerClientId == networkManager.LocalClientId)
            {
                //We are current owner.
                networkObject.InvokeBehaviourOnLostOwnership();
            }

            networkObject.OwnerClientId = OwnerClientId;

            if (OwnerClientId == networkManager.LocalClientId)
            {
                //We are new owner.
                networkObject.InvokeBehaviourOnGainedOwnership();
            }

            networkManager.NetworkMetrics.TrackOwnershipChangeReceived(context.SenderId, networkObject, context.MessageSize);
        }
    }
}
