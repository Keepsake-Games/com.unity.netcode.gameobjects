using Unity.Netcode.Interest;

namespace Unity.Netcode
{
    public class AddAllInterestKernel : IInterestKernel<NetworkObject>
    {
        public void OnObjectAdded(NetworkObject obj)
        {
        }

        public void OnObjectRemoved(NetworkObject obj)
        {
        }

        public bool QueryFor(NetworkObject clientNetworkObject, NetworkObject obj)
        {
            return true;
        }
    }
}
