namespace MVC_SSL_Chat.Internal.Interfaces
{
    /// <summary>
    /// The interface of the current time provider.
    /// </summary>
    public interface IClock
    {
        /// <summary>
        /// Get current universal time.
        /// </summary>
        DateTime UtcNow { get; }
    }
}
