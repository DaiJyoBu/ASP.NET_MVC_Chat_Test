using MVC_SSL_Chat.Models;

namespace MVC_SSL_Chat.Internal.Interfaces
{
    /// <summary>
    /// The interface of the buffering service - the service that accumulates the new messages and flushes them into the database under certain conditions
    /// </summary>
    public interface IBufferingService
    {
        /// <summary>
        /// Append the new message to be flushed.
        /// </summary>
        /// <param name="messageModel">The new message.</param>
        void Append(MessageModel messageModel);
    }
}
