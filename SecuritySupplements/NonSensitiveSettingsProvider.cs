using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using SecuritySupplements.Contracts;
using Storage.Contracts;

namespace SecuritySupplements
{
    /// <summary>
    /// The implementation for the provider of the settings used by the storage and the settings used by the sensitive data reader.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    public class NonSensitiveSettingsProvider( ILoggerFactory loggerFactory ) : IStorageSettingsProvider, IReaderSettingsProvider
    {
        private const string SaveFrequencyKey = "SaveFrequency";
        private const int SaveFrequencyDefaultValue = 1000;
        private const string RefreshingFrequencyKey = "RefreshingFrequency";
        private const int RefreshingFrequencyDefaultValue = 15000;
        private const string ReadingAttemptsMaxKey = "VaultReadingAttemptsMax";
        private const int ReadingAttemptsMaxDefaultValue = 5;
        private const string ReadingDelayKey = "VaultReadingDelay";
        private const int ReadingDelayDefaultValue = 1000;

        private readonly ILogger _logger = loggerFactory.CreateLogger( nameof( NonSensitiveSettingsProvider ) );

        public int SavingFrequencyInMilliseconds { get; private set; }

        public int RefreshingFrequencyInMilliseconds { get; private set; }
        
        public int ReadingAttemptsMax { get; private set; }

        public int ReadingDelay { get; private set; }

        /// <summary>
        /// Reading the executing assembly's configuration for the non-localizable settings.
        /// </summary>
        /// <param name="configuration">Executing assembly's configuration</param>
        public void ReadAppConfig( IConfiguration configuration )
        {
            TryReadingIntegerSetting( key: SaveFrequencyKey, 
                configuration: configuration,
                setValueIfSuccessfulRead: v => SavingFrequencyInMilliseconds = v,
                setValueIfFailedRead: () => SavingFrequencyInMilliseconds = SaveFrequencyDefaultValue );

            TryReadingIntegerSetting( key: RefreshingFrequencyKey,
                configuration: configuration,
                setValueIfSuccessfulRead: v => RefreshingFrequencyInMilliseconds = v,
                setValueIfFailedRead: () => RefreshingFrequencyInMilliseconds = RefreshingFrequencyDefaultValue );

            TryReadingIntegerSetting( key: ReadingAttemptsMaxKey,
                configuration: configuration,
                setValueIfSuccessfulRead: v => ReadingAttemptsMax = v,
                setValueIfFailedRead: () => ReadingAttemptsMax = ReadingAttemptsMaxDefaultValue );

            TryReadingIntegerSetting( key: ReadingDelayKey,
                configuration: configuration,
                setValueIfSuccessfulRead: v => ReadingDelay = v,
                setValueIfFailedRead: () => ReadingDelay = ReadingDelayDefaultValue );
        }

        private void TryReadingIntegerSetting( 
            string key, IConfiguration configuration, Action<int> setValueIfSuccessfulRead, Action setValueIfFailedRead )
        {
            _logger.LogTrace( $"Attempting to read the { key } from the executing assembly's configuration settings." );

            try
            {
                var settingValue = configuration[ key ];
                if( settingValue != null && int.TryParse( settingValue, out var intValue )) 
                {
                    setValueIfSuccessfulRead( intValue );
                    _logger.LogTrace( $"{ key } was successfully read from the executing assembly's configuration settings." );
                }
            }
            catch( Exception ex )
            {
                _logger.LogError( ex, $"Error reading { key } from the executing assembly's configuration settings. Applying default value." );
                setValueIfFailedRead();
            }
        }
    }
}
