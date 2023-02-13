namespace Unity.Netcode
{
    // KEEPSAKE FIX made public
    public interface IMessageSender
    {
        void Send(ulong clientId, NetworkDelivery delivery, FastBufferWriter batchData);
    }
}
