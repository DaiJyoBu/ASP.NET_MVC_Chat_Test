using DomainModels;

namespace Storage.Contracts
{
    /// <summary>
    /// The interface of the service used for the database access operations.
    /// </summary>
    public interface IDbService
    {
        /// <summary>
        /// Get all messages from all users that were sent in the specified time interval.
        /// </summary>
        /// <param name="from">The starting point of the interval.</param>
        /// <param name="from">The ending point of the interval.</param>
        /// <returns></returns>
        Task<IEnumerable<ChatMessage>> GetMessages( DateTime from, DateTime to );

        /// <summary>
        /// Save messages that were created since the previous save.
        /// </summary>
        /// <param name="newMessages">Newly sent messages.</param>
        /// <returns></returns>
        Task SaveChangesAsync( IEnumerable<ChatMessage> newMessages );
    }
}
