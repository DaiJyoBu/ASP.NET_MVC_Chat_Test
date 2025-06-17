using DomainModels;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Storage.Contracts;

namespace Storage
{
    /// <summary>
    /// The implementation of the service used for the database access operations.
    /// </summary>
    internal class DbService( ILoggerFactory loggerFactory, IChatDbContextFactory dbContextFactory ) : IDbService
    {
        private readonly IChatDbContextFactory _dbContextFactory = dbContextFactory;
        private readonly ILogger _logger = loggerFactory.CreateLogger( nameof( DbService ) );

        /// <inheritdoc />
        public async Task<IEnumerable<ChatMessage>> GetMessages( DateTime from, DateTime to )
        {
            _logger.LogTrace( $"Getting the messages for the period from { from } to { to }." );
            try
            {
                using var storageContext = _dbContextFactory.Create();
                await using(storageContext.ConfigureAwait( false ))
                {
                    var result = storageContext.ChatMessages.Include( m => m.Author ).Where( message => message.CreatedTime >= from && message.CreatedTime <= to )
                        .ToArray();
                    _logger.LogTrace( $"The messages are obtained successfully." );
                    return result;

                }
            }
            catch ( Exception ex )
            {
                _logger.LogError( ex, "The error occured while getting the messages." );
                throw;
            }
        }

        /// <inheritdoc />
        public async Task SaveChangesAsync( IEnumerable<ChatMessage> newMessages )
        {
            _logger.LogTrace( $"Saving the changes." );

            try
            {
                using var storageContext = _dbContextFactory.Create();

                await using ( storageContext.ConfigureAwait( false ) )
                {
                    storageContext.ChatMessages.AddRange( newMessages );

                    await storageContext
                        .BulkSaveChangesAsync()
                        .ConfigureAwait( false );

                    _logger.LogTrace( $"The messages are saved successfully." );
                }
            }
            catch ( Exception ex )
            {
                _logger.LogError( ex, "The error occured while saving the messages." );
                throw;
            }
        }
    }
}
