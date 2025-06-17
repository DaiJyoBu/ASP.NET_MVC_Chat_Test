using Microsoft.EntityFrameworkCore;
using SecuritySupplements.Contracts;
using System.Reflection;

namespace Storage
{
    /// <summary>
    /// The implementation of the database context factory interface.
    /// </summary>
    internal class ChatDbContextFactory( ISensitiveDataProvider sensitiveDataProvider ) : IChatDbContextFactory
    {
        private readonly ISensitiveDataProvider _sensitiveDataProvider = sensitiveDataProvider;

        /// <inheritdoc />
        public ChatDbContext Create()
        {
            var options =
                new DbContextOptionsBuilder<ChatDbContext>().UseNpgsql( _sensitiveDataProvider.DatabaseConnectionString,
                    b => b.MigrationsAssembly( Assembly.GetExecutingAssembly().GetName().Name) ).Options;

            return new ChatDbContext(options);
        }
    }
}
