using MVC_SSL_Chat.Internal.Interfaces;

namespace MVC_SSL_Chat.Internal.Implementations
{
    /// <summary>
    /// The implementation of the current time provider.
    /// </summary>
    public class SystemClock : IClock
    {
        /// <inheritdoc />
        public DateTime UtcNow { get { return DateTime.UtcNow; } }
    }
}
