using MVC_SSL_Chat.Models;

namespace MVC_SSL_Chat.Internal.Interfaces
{
    /// <summary>
    /// The interface for the dispatcher of the messages travelling across the stream.
    /// </summary>
    public interface IChatMessageDispatcher
    {
        /// <summary>
        /// Register the listener method to be used by the dispatcher.
        /// </summary>
        /// <param name="listener">The listener method.</param>
        void Register ( Action<SendMessageRequestModel> listener );

        /// <summary>
        /// Unregister the listener method.
        /// </summary>
        /// <param name="listener">The listener method.</param>
        void Unregister( Action<SendMessageRequestModel> listener );

        /// <summary>
        /// Dispatch the message across all registered listeners.
        /// </summary>
        /// <param name="message">The message to be dispatched.</param>
        void Dispatch( SendMessageRequestModel message );
    }
}
