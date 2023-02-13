using System.Collections.Generic;
using UnityEngine;
using UnityEditor;

namespace Unity.Netcode.Editor
{
    [CustomEditor(typeof(NetworkObject), true)]
    [CanEditMultipleObjects]
    public class NetworkObjectEditor : UnityEditor.Editor
    {
        private bool m_Initialized;
        private NetworkObject m_NetworkObject;
        private bool m_ShowObservers;

        // KEEPSAKE FIX
        private string m_PendingNewOwnerStr;

        private void Initialize()
        {
            if (m_Initialized)
            {
                return;
            }

            m_Initialized = true;
            m_NetworkObject = (NetworkObject)target;
        }

        public override void OnInspectorGUI()
        {
            Initialize();

            // KEEPSAKE FIX - a bit tired of this dynamic inspector of Unity's, just show some useful info
            var guiEnabled = GUI.enabled;
            GUI.enabled = false;
            EditorGUILayout.TextField(nameof(NetworkObject.GlobalObjectIdHash), m_NetworkObject.GlobalObjectIdHash.ToString());
            EditorGUILayout.TextField(nameof(NetworkObject.NetworkObjectId), m_NetworkObject.NetworkObjectId.ToString());
            EditorGUILayout.TextField(nameof(NetworkObject.OwnerClientId), m_NetworkObject.OwnerClientId.ToString());
            EditorGUILayout.Toggle(nameof(NetworkObject.IsSpawned), m_NetworkObject.IsSpawned);
            EditorGUILayout.Toggle(nameof(NetworkObject.IsAttached), m_NetworkObject.IsAttached); // KEEPSAKE FIX
            EditorGUILayout.Toggle(nameof(NetworkObject.IsLocalPlayer), m_NetworkObject.IsLocalPlayer);
            EditorGUILayout.Toggle(nameof(NetworkObject.IsOwner), m_NetworkObject.IsOwner);
            EditorGUILayout.Toggle(nameof(NetworkObject.IsOwnedByServer), m_NetworkObject.IsOwnedByServer);
            EditorGUILayout.Toggle(nameof(NetworkObject.IsPlayerObject), m_NetworkObject.IsPlayerObject);
            if (m_NetworkObject.IsSceneObject.HasValue)
            {
                EditorGUILayout.Toggle(nameof(NetworkObject.IsSceneObject), m_NetworkObject.IsSceneObject.Value);
            }
            else
            {
                EditorGUILayout.TextField(nameof(NetworkObject.IsSceneObject), "null");
            }
            EditorGUILayout.Toggle(nameof(NetworkObject.DestroyWithScene), m_NetworkObject.DestroyWithScene);
            EditorGUILayout.TextField(nameof(NetworkObject.NetworkManager), m_NetworkObject.NetworkManager == null ? "null" : m_NetworkObject.NetworkManager.gameObject.name);
            EditorGUILayout.Space();
            EditorGUILayout.Space();
            GUI.enabled = guiEnabled;
            // END KEEPSAKE FIX

            // KEEPSAKE FIX - check IsAttached and not IsSpawned
            if (EditorApplication.isPlaying && !m_NetworkObject.IsAttached && m_NetworkObject.NetworkManager != null && m_NetworkObject
            .NetworkManager.IsServer)
            {
                EditorGUILayout.BeginHorizontal();
                EditorGUILayout.LabelField(new GUIContent("Spawn", "Spawns the object across the network"));
                if (GUILayout.Toggle(false, "Spawn", EditorStyles.miniButtonLeft))
                {
                    m_NetworkObject.Spawn();
                    EditorUtility.SetDirty(target);
                }

                EditorGUILayout.EndHorizontal();
            }
            else if (EditorApplication.isPlaying && m_NetworkObject.IsAttached) // KEEPSAKE FIX - check IsAttached and not IsSpawned
            {
                guiEnabled = GUI.enabled;
                GUI.enabled = false;
                EditorGUILayout.TextField(nameof(NetworkObject.GlobalObjectIdHash), m_NetworkObject.GlobalObjectIdHash.ToString());
                EditorGUILayout.TextField(nameof(NetworkObject.NetworkObjectId), m_NetworkObject.NetworkObjectId.ToString());
                EditorGUILayout.TextField(nameof(NetworkObject.OwnerClientId), m_NetworkObject.OwnerClientId.ToString());
                EditorGUILayout.Toggle(nameof(NetworkObject.IsSpawned), m_NetworkObject.IsSpawned);
                EditorGUILayout.Toggle(nameof(NetworkObject.IsAttached), m_NetworkObject.IsAttached); // KEEPSAKE FIX
                EditorGUILayout.Toggle(nameof(NetworkObject.IsLocalPlayer), m_NetworkObject.IsLocalPlayer);
                EditorGUILayout.Toggle(nameof(NetworkObject.IsOwner), m_NetworkObject.IsOwner);
                EditorGUILayout.Toggle(nameof(NetworkObject.IsOwnedByServer), m_NetworkObject.IsOwnedByServer);
                EditorGUILayout.Toggle(nameof(NetworkObject.IsPlayerObject), m_NetworkObject.IsPlayerObject);
                if (m_NetworkObject.IsSceneObject.HasValue)
                {
                    EditorGUILayout.Toggle(nameof(NetworkObject.IsSceneObject), m_NetworkObject.IsSceneObject.Value);
                }
                else
                {
                    EditorGUILayout.TextField(nameof(NetworkObject.IsSceneObject), "null");
                }
                EditorGUILayout.Toggle(nameof(NetworkObject.DestroyWithScene), m_NetworkObject.DestroyWithScene);
                EditorGUILayout.TextField(nameof(NetworkObject.NetworkManager), m_NetworkObject.NetworkManager == null ? "null" : m_NetworkObject.NetworkManager.gameObject.name);
                GUI.enabled = guiEnabled;

                if (m_NetworkObject.NetworkManager != null && m_NetworkObject.NetworkManager.IsServer)
                {
                    m_ShowObservers = EditorGUILayout.Foldout(m_ShowObservers, "Observers");

                    if (m_ShowObservers)
                    {
                        HashSet<ulong>.Enumerator observerClientIds = m_NetworkObject.GetObservers();

                        EditorGUI.indentLevel += 1;

                        while (observerClientIds.MoveNext())
                        {
                            if (m_NetworkObject.NetworkManager.ConnectedClients[observerClientIds.Current].PlayerObject != null)
                            {
                                EditorGUILayout.ObjectField($"ClientId: {observerClientIds.Current}", m_NetworkObject.NetworkManager.ConnectedClients[observerClientIds.Current].PlayerObject, typeof(GameObject), false);
                            }
                            else
                            {
                                EditorGUILayout.TextField($"ClientId: {observerClientIds.Current}", EditorStyles.label);
                            }
                        }

                        EditorGUI.indentLevel -= 1;
                    }

                    // KEEPSAKE FIX - change owner UI
                    if (string.IsNullOrEmpty(m_PendingNewOwnerStr))
                    {
                        m_PendingNewOwnerStr = m_NetworkObject.OwnerClientId.ToString();
                    }
                    GUILayout.BeginHorizontal();
                    GUILayout.Label("Change owner");
                    GUILayout.FlexibleSpace();
                    m_PendingNewOwnerStr = EditorGUILayout.TextField(nameof(NetworkObject.OwnerClientId), m_PendingNewOwnerStr);
                    if (GUILayout.Button("Apply") && ulong.TryParse(m_PendingNewOwnerStr, out var newOwner))
                    {
                        m_NetworkObject.ChangeOwnership(newOwner);
                    }
                    GUILayout.EndHorizontal();
                    // END KEEPSAKE FIX
                }
            }
            else
            {


                base.OnInspectorGUI();

                guiEnabled = GUI.enabled;
                GUI.enabled = false;
                EditorGUILayout.TextField(nameof(NetworkObject.GlobalObjectIdHash), m_NetworkObject.GlobalObjectIdHash.ToString());
                EditorGUILayout.TextField(nameof(NetworkObject.NetworkManager), m_NetworkObject.NetworkManager == null ? "null" : m_NetworkObject.NetworkManager.gameObject.name);
                GUI.enabled = guiEnabled;

                // KEEPSAKE FIX - added button to refresh id
                if (GUILayout.Button($"Regenerate {nameof(m_NetworkObject.GlobalObjectIdHash)}"))
                {
                    m_NetworkObject.RegenerateGlobalObjectIdHash();
                }
            }
        }
    }
}
