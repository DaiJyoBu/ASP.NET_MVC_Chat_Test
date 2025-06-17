
namespace Storage
{
    /// <summary>
    /// The interface for the database context factory.
    /// </summary>
    internal interface IChatDbContextFactory
    {
        /// <summary>
        /// Create the database context.
        /// </summary>
        ChatDbContext Create();
    }
}
