using System.Collections.Generic;

namespace Unity.Netcode.Interest
{
    // interest *system* instead of interest node ?
    public class InterestManager<TObject>
    {
        private InterestNodeStatic<TObject> m_DefaultInterestNode = new InterestNodeStatic<TObject>();

        // KEEPSAKE FIX - remember interest of those not marked as dirty since last time
        private Dictionary<TObject, List<TObject>> m_CachedResults = new();

        // KEEPSAKE FIX - reused as "working memory"
        private HashSet<TObject> m_WorkingResults = new();

        public InterestManager()
        {
            // This is the node objects will be added to if no replication group is
            //  specified, which means they always get replicated
            m_ChildNodes = new HashSet<IInterestNode<TObject>> { m_DefaultInterestNode };
        }

        // Trigger the Interest system to do an update sweep on any Interest nodes
        //  I am associated with
        public void UpdateObject(ref TObject obj)
        {
            if (m_InterestNodesMap.TryGetValue(obj, out var nodes))
            {
                foreach (var node in nodes)
                {
                    node.UpdateObject(obj);
                }
            }
        }

        // KEEPSAKE FIX
        public void MarkInterestDirty(ref TObject obj)
        {
            if (m_InterestNodesMap.TryGetValue(obj, out var nodes))
            {
                foreach (var node in nodes)
                {
                    node.MarkInterestDirty(obj);
                }
            }

            foreach (var cached in m_CachedResults.Values)
            {
                cached.Remove(obj);
            }
        }

        public void ClearInterestDirty()
        {
            foreach (var c in m_ChildNodes)
            {
                c.ClearInterestDirty();
            }
        }

        public void OnClientDisconnected(TObject client)
        {
            m_CachedResults.Remove(client);
        }
        // END KEEPSAKE FIX

        public void AddObject(ref TObject obj)
        {
            // If this new object has no associated Interest Nodes, then we put it in the
            //  default node, which all clients will then get.
            //
            // That is, if you don't opt into the system behavior is the same as before
            //  the Interest system was added

            if (m_InterestNodesMap.TryGetValue(obj, out var nodes))
            {
                // I am walking through each of the interest nodes that this object has
                foreach (var node in nodes)
                {
                    // the Interest Manager lazily adds nodes to itself when it sees
                    //  new nodes that associate with the objects being added
                    m_ChildNodes.Add(node);
                }
            }
            else
            {
                // if the object doesn't have any nodes, we assign it to the default node
                AddDefaultInterestNode(obj);
            }

            // KEEPSAKE FIX
            MarkInterestDirty(ref obj);
        }

        public void AddDefaultInterestNode(TObject obj)
        {
            AddInterestNode(ref obj, m_DefaultInterestNode);
        }

        public void RemoveObject(ref TObject obj)
        {
            if (m_InterestNodesMap.TryGetValue(obj, out var nodes))
            {
                foreach (var node in nodes)
                {
                    if (node == null)
                    {
                        continue;
                    }

                    node.RemoveObject(obj);
                }

                m_InterestNodesMap.Remove(obj);
            }

            // KEEPSAKE FIX
            foreach (var cached in m_CachedResults.Values)
            {
                cached.Remove(obj);
            }
        }

        // KEEPSAKE FIX - changed results from ref HashSet to out ReadOnlyList since we algorithmically ensure no doubles, and now re-use memory internally
        public void QueryFor(ref TObject client, out IReadOnlyList<TObject> results)
        {
            // cached results that keep their interest from last time checked
            if (!m_CachedResults.TryGetValue(client, out var cached))
            {
                cached = new List<TObject>();
                m_CachedResults[client] = cached;
            }

            // query to get interest of "dirty" objects (tracked internally per node)
            m_WorkingResults.Clear();
            foreach (var c in m_ChildNodes)
            {
                c.QueryFor(client, m_WorkingResults);
            }

            // save queried objects to the cache
            // will be removed from cache when flagged as dirty
            cached.AddRange(m_WorkingResults);
            m_WorkingResults.Clear();

            results = cached;
        }

        public void AddInterestNode(ref TObject obj, IInterestNode<TObject> node)
        {
            node.AddObject(obj);

            if (!m_InterestNodesMap.TryGetValue(obj, out var nodes))
            {
                m_InterestNodesMap[obj] = new List<IInterestNode<TObject>>();
                m_InterestNodesMap[obj].Add(node);
            }
            else
            {
                if (!nodes.Contains(node))
                {
                    nodes.Add(node);
                }
            }
        }

        public void RemoveInterestNode(ref TObject obj, IInterestNode<TObject> node)
        {
            node.RemoveObject(obj);
            if (m_InterestNodesMap.TryGetValue(obj, out var nodes))
            {
                if (nodes.Contains(node))
                {
                    nodes.Remove(node);
                }
            }
        }

        private HashSet<IInterestNode<TObject>> m_ChildNodes;

        private Dictionary<TObject, List<IInterestNode<TObject>>> m_InterestNodesMap =
            new Dictionary<TObject, List<IInterestNode<TObject>>>();
    }
}
