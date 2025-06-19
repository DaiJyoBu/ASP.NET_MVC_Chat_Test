using MVC_SSL_Chat.Internal.Interfaces;
using MVC_SSL_Chat.Models;

namespace MVC_SSL_Chat.Internal.Implementations
{
    /// <summary>
    /// The implementation of the dispatcher of the messages travelling across the stream.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    public class ChatMessageDispatcher( ILoggerFactory loggerFactory ) : IChatMessageDispatcher
    {
        private readonly ILogger _logger = loggerFactory.CreateLogger( nameof( ChatMessageDispatcher ) );

        private readonly List<Action<SendMessageRequestModel>> _listeners = new();

        /// <inheritdoc />
        public void Register( Action<SendMessageRequestModel> listener )
        {
            _logger.LogTrace( "Registering the listener." );
            _listeners.Add( listener );
            _logger.LogTrace( "The listener is registered." );
        }

        /// <inheritdoc />
        public void Unregister( Action<SendMessageRequestModel> listener )
        {
            _logger.LogTrace( "Unregistering the listener." );
            _listeners.Remove( listener );
            _logger.LogTrace( "The listener is unregistered." );
        }

        /// <inheritdoc />
        public void Dispatch( SendMessageRequestModel message )
        {
            _logger.LogTrace( "Dispatching the message." );
            try
            {
                _listeners.ForEach( l => DispatchSingleMessage( l, message ) );
                _logger.LogTrace( "The message is dispatched." );
            }
            catch ( Exception ex )
            {
                _logger.LogError( ex, "The dispatching was interrupted due to the error." );
            }
        }

        private void DispatchSingleMessage( Action<SendMessageRequestModel> listener, SendMessageRequestModel message )
        {
            try
            {
                listener( message );
            }
            catch (Exception ex )
            {
                _logger.LogError(ex, "The error occured while dispatching the message." );
            }
        }
    }
}
