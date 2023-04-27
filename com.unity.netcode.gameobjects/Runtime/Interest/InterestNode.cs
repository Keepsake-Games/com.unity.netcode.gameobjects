using System.Collections.Generic;

namespace Unity.Netcode.Interest
{
    public interface IInterestNode<TObject>
    {
        // KEEPSAKE FIX - added `subset` to not iterate all objects every tick
        public void QueryFor(TObject client, HashSet<TObject> results);
        public void AddObject(TObject obj);
        public void RemoveObject(TObject obj);
        public void UpdateObject(TObject obj);
        public void AddAdditiveKernel(IInterestKernel<TObject> kernel);
        public void AddSubtractiveKernel(IInterestKernel<TObject> kernel);

        // KEEPSAKE FIX - flag interest as "dirty" to only recompute on a subset
        void MarkInterestDirty(TObject obj);
        void ClearInterestDirty();

        // KEEPSAKE FIX - added to cleanup clients when they d/c
        void ForgetClient(TObject client);
    };

    public interface IInterestKernel<TObject>
    {
        // KEEPSAKE FIX added some methods that gets invoked when object added to parent InterestNode for one-time setup
        public void OnObjectAdded(TObject obj);
        public void OnObjectRemoved(TObject obj);

        public bool QueryFor(TObject client, TObject obj);
    }
}
