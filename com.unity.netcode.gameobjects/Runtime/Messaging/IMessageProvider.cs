using System.Collections.Generic;

namespace Unity.Netcode
{
    // KEEPSAKE FIX made public
    public interface IMessageProvider
    {
        List<MessagingSystem.MessageWithHandler> GetMessages();
    }
}
