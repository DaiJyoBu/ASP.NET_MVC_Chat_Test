using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace SecuritySupplements.HashicorpVault
{
    /// <summary>
    /// The reader that obtains the the Hashicorp Vault access credentials from wherever they are.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    public class VaultCredentialsReader( ILoggerFactory loggerFactory )
    {
        private const string ConfigurationUserSecretsSectionName = "HashicorpVault";
        private const string CredentialToken = "HashicorpVaultCredentialToken";
        private const string CredentialAddress = "HashicorpVaultCredentialAddress";
        private const string CredentialPath = "HashicorpVaultCredentialPath";
        private const string CredentialMountPoint = "HashicorpVaultCredentialMountPoint";

        private readonly ILogger _logger = loggerFactory.CreateLogger( nameof( VaultCredentialsReader ) );

        /// <summary>
        /// Read the Hashicorp Vault access credentials from the current configuration's user secrets.
        /// </summary>
        /// <param name="configuration">current configuration</param>
        /// <returns>If the credentials were successfully read - wrapper for Vault credentials. Null otherwise.</returns>
        public VaultCredentials? ReadFromUserSecrets( IConfiguration configuration )
        {
            try
            {
                _logger.LogTrace( $"Attempting to read the {ConfigurationUserSecretsSectionName} configuration section." );
                var section = configuration.GetSection( ConfigurationUserSecretsSectionName );

                if(!section.Exists())
                {
                    _logger.LogCritical( $"Configuration section {ConfigurationUserSecretsSectionName} does not exist! Application cannot start." );
                    return null;
                }
                var result = section.Get<VaultCredentials>()!;

                _logger.LogTrace( $"Configuration section {ConfigurationUserSecretsSectionName} was successfully read." );
                return result;
            }
            catch( Exception ex )
            {
                _logger.LogCritical( ex, $"Error reading the configuration section {ConfigurationUserSecretsSectionName}. See exception details. Application cannot start." );
                return null;
            }
        }

        /// <summary>
        /// Read the Hashicorp Vault access credentials from the environment's variables.
        /// </summary>
        /// <returns>If the credentials were successfully read - wrapper for Vault credentials. Null otherwise.</returns>
        public VaultCredentials? ReadFromEnvironmentVariables()
        {
            VaultCredentials vaultCredentials = new VaultCredentials();

            _logger.LogTrace( $"Attempting to read the environment variables." );

            if( !SetFromEnvironmentValue( CredentialToken, v => vaultCredentials.Token = v ) )
            {
                return null;
            }

            if( !SetFromEnvironmentValue( CredentialAddress, v => vaultCredentials.Address = v ) )
            {
                return null;
            }

            if( !SetFromEnvironmentValue( CredentialPath, v => vaultCredentials.Path = v ) )
            {
                return null;
            }

            if( !SetFromEnvironmentValue( CredentialMountPoint, v => vaultCredentials.MountPoint = v ) )
            {
                return null;
            }

            _logger.LogTrace( $"The environment variables were read successfully." );

            return vaultCredentials;
        }

        private bool SetFromEnvironmentValue( string environmentVariableName, Action<string?> setEnvironmentVariableValue )
        {
            try
            {
                _logger.LogTrace( $"Attempting to read the environment variable { environmentVariableName }." );
                
                var value = Environment.GetEnvironmentVariable( environmentVariableName );

                _logger.LogTrace( $"Environment variable { environmentVariableName } was read." );
                
                setEnvironmentVariableValue( value );

                _logger.LogTrace( $"Environment variable { environmentVariableName } was set." );

                return true;
            }
            catch( Exception ex )
            {
                _logger.LogCritical( ex, $"Error reading the environment variable { environmentVariableName }. See exception details. Application cannot start." );

                return false;
            }
        }
    }
}
