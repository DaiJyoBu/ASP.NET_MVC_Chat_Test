using MVC_SSL_Chat.Internal.Interfaces;
using MVC_SSL_Chat.Models;
using Storage.Contracts;
using System.Collections.Concurrent;

namespace MVC_SSL_Chat.Internal.Implementations
{
    /// <summary>
    /// The implementation of the buffering service.
    /// </summary>
    internal sealed class BufferingService : IBufferingService, IDisposable
    {
        private ConcurrentBag<MessageModel> _mainBuffer = new();
        private ConcurrentBag<MessageModel> _spareBuffer = new();

        private readonly ReaderWriterLockSlim _lock = new();
        private readonly IDbService _dbService;
        private readonly Timer _timer;

        private int _flushProgressIndicator;

        /// <summary>
        /// The constructor.
        /// </summary>
        /// <param name="dbService">The database service.</param>
        /// <param name="settingsProvider">The provider of the storage settings.</param>
        public BufferingService( IDbService dbService, IStorageSettingsProvider settingsProvider )
        {
            _dbService = dbService;

            _timer = new Timer( FlushAsync, null, 0, settingsProvider.SavingFrequencyInMilliseconds );
        }

        /// <inheritdoc />
        public void Append( MessageModel message )
        {
            bool readerLockTaken = false;

            try
            {
                _lock.EnterReadLock();
                readerLockTaken = true;
                _mainBuffer.Add( message );
            }
            finally
            {
                if (readerLockTaken)
                {
                    _lock.ExitReadLock();
                }
            }
        }

        private async void FlushAsync( object? stub )
        {
            var isFlushHappening = Interlocked.CompareExchange( ref _flushProgressIndicator, 1, 0 );

            if (isFlushHappening == 1)
            {
                return; // The previous flush is in progress, so the current flush is postponed
            }
            bool writerLockTaken = false;

            try
            {
                _lock.EnterWriteLock();
                writerLockTaken = true;

                ( _spareBuffer, _mainBuffer ) = ( _mainBuffer, _spareBuffer );
            }
            finally
            {
                if ( writerLockTaken )
                {
                    _lock.ExitWriteLock();
                }
            }

            await SaveMessagesAsync( _spareBuffer );
            _spareBuffer.Clear();
            Interlocked.Exchange( ref _flushProgressIndicator, 0 );
        }

        private async Task SaveMessagesAsync( IEnumerable<MessageModel> messages )
        {
            var newMessages = messages.Select( m => m.ToDatabaseEntity() ).ToArray();

            await _dbService.SaveChangesAsync( newMessages );
        }

        public void Dispose()
        {
            _timer.Dispose();
        }
    }
}
