#if UNITY_EDITOR
using UnityEditor;
using UnityEditor.SceneManagement;
using UnityEngine.SceneManagement;

namespace Unity.Netcode.SceneManagement
{

// KEEPSAKE FIX - util class to allow NetworkObject to avoid running OnValidate when opening a scene.
//                this is because any changes made will not properly mark the scene as dirty, even though things will "look right" it won't persist on disk
[InitializeOnLoad]
public static class NetworkEditorSceneLoadingTracker
{
    public static bool IsOpeningScene { get; private set; }

    static NetworkEditorSceneLoadingTracker()
    {
        EditorSceneManager.sceneOpening += OnSceneOpening;
        EditorSceneManager.sceneOpened += OnSceneOpened;
    }

    private static void OnSceneOpening(string path, OpenSceneMode mode)
    {
        IsOpeningScene = true;
    }

    private static void OnSceneOpened(Scene scene, OpenSceneMode mode)
    {
        IsOpeningScene = false;
    }
}

}
#endif
