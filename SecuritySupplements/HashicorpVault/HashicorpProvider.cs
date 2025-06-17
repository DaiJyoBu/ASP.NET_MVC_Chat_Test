using SecuritySupplements.Contracts;
using VaultSharp.V1.AuthMethods.Token;
using VaultSharp;
using VaultSharp.V1.Commons;
using SecuritySupplements.HashicorpVault;
using Microsoft.Extensions.Logging;

namespace SecuritySupplements
{
    /// <summary>
    /// The implementation of the provider of the protection critical data.
    /// </summary>
    public class HashicorpProvider( ILoggerFactory loggerFactory, IReaderSettingsProvider readerSettingsProvider ) : ISensitiveDataProvider
    {
        private readonly ILogger _logger = loggerFactory.CreateLogger( nameof( HashicorpProvider ) );
        private readonly IReaderSettingsProvider readerSettingsProvider = readerSettingsProvider;

        public string DatabaseConnectionString { get; private set; } = string.Empty;

        public string ConfirmationEmailSmtpHost { get; private set; } = string.Empty;

        public int ConfirmationEmailSmtpPort { get; private set; }

        public string ConfirmationEmailAddress { get; private set; } = string.Empty;

        public string ConfirmationEmailPassword { get; private set; } = string.Empty;

        /// <summary>
        /// The indicator that the secrets data was loaded successfully from the Vault.
        /// </summary>
        public bool IsRead { get; private set; }

        /// <summary>
        /// Load secrets data from the Vault.
        /// </summary>
        /// <param name="vaultOptions">The vault secrets access credentials.</param>
        /// <returns></returns>
        public async Task LoadVaultData( VaultCredentials vaultOptions )
        {
            var vaultToken = vaultOptions.Token;
            var authMethod = new TokenAuthMethodInfo( vaultToken );
            var vaultClientSettings = new VaultClientSettings( vaultOptions.Address, authMethod );

            var client = new VaultClient( vaultClientSettings );

            _logger.Log( LogLevel.Trace, "Requesting the sensitive data from Vault." );

            string logAction = string.Empty;

            for(int attempt = 1; attempt <= readerSettingsProvider.ReadingAttemptsMax; attempt++)
            {
                try
                {
                    logAction = "reading the section";
                    Secret<SecretData> secrets = await client.V1.Secrets.KeyValue.V2.ReadSecretAsync(
                      path: vaultOptions.Path,
                      version: null,
                      mountPoint: vaultOptions.MountPoint );
                    var secretsData = secrets.Data.Data;

                    _logger.Log( LogLevel.Trace, "Sensitive section acquired from Vault. Reading the data values." );

                    logAction = "reading the database connection";
                    DatabaseConnectionString = secretsData[ "DatabaseConnection" ].ToString()!;

                    logAction = "reading the smtp host";
                    ConfirmationEmailSmtpHost = secretsData[ "EmailSmtpHost" ].ToString()!;

                    logAction = "reading the smtp port";
                    ConfirmationEmailSmtpPort = int.Parse( secretsData[ "EmailSmtpPort" ].ToString()! );

                    logAction = "reading the email address";
                    ConfirmationEmailAddress = secretsData[ "EmailAddress" ].ToString()!;

                    logAction = "reading the email password";
                    ConfirmationEmailPassword = secretsData[ "EmailPassword" ].ToString()!;

                    _logger.Log( LogLevel.Trace, "Sensitive data acquired from Vault." );
                    IsRead = true;
                    return;
                }
                catch(Exception ex)
                {
                    _logger.LogError( ex, $"Attempt { attempt }/{ readerSettingsProvider.ReadingAttemptsMax } - Failed reading secrets from Vault." );
                    await Task.Delay( readerSettingsProvider.ReadingDelay );
                }
            }
            _logger.LogCritical( $"Failed to obtain secrets from Vault after { readerSettingsProvider.ReadingAttemptsMax } attempts. See log for error details." );
        }
    }
}
