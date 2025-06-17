using Microsoft.Extensions.Logging;
using SecuritySupplements.Contracts;
using System.Reflection;
using System.Resources;

namespace Email
{
    /// <summary>
    /// The interface of the provider of the title and the content for the email that is being sent for the new account activation.
    /// </summary>
    internal class ConfirmationEmailProvider : IConfirmationEmailProvider
    {
        private const string ConfirmationEmailTitleKey = "ConfirmationEmailTitle";
        private const string ConfirmationEmailTitleDefaultValue = "Confirm your email.";
        private const string ConfirmationEmailBodyKey = "ConfirmationEmailBody";
        private const string ConfirmationEmailBodyDefaultValue = "Please confirm your account by <a href='{0}'>clicking here</a>.";
        private static readonly string LocalizationFileName = $"{AppDomain.CurrentDomain.FriendlyName}.Resources.Localization";
        private readonly ILogger _logger;

        /// <summary>
        /// The constructor.
        /// </summary>
        /// <param name="loggerFactory">The logger factory.</param>
        public ConfirmationEmailProvider( ILoggerFactory loggerFactory )
        {
            _logger = loggerFactory.CreateLogger( nameof( ConfirmationEmailProvider ) );

            try
            {
                _logger.LogTrace( $"Attempting to read the {ConfirmationEmailTitleKey} and {ConfirmationEmailBodyKey} from application resources." );

                var assembly = Assembly.GetEntryAssembly();
                var rm = new ResourceManager( LocalizationFileName, assembly! );
                ConfirmationEmailTitle = rm.GetString( ConfirmationEmailTitleKey );
                ConfirmationEmailBody = rm.GetString( ConfirmationEmailBodyKey );
                rm.ReleaseAllResources();

                _logger.LogTrace( $"{ ConfirmationEmailTitleKey } and { ConfirmationEmailBodyKey } were successfully read from application resources." );
            }
            catch( Exception ex )
            {
                _logger.LogError( ex, $"Error reading { ConfirmationEmailTitleKey } and { ConfirmationEmailBodyKey } from application resources. Applying default values." );
                ConfirmationEmailTitle = ConfirmationEmailTitleDefaultValue;
                ConfirmationEmailBody = ConfirmationEmailBodyDefaultValue;
            }
        }

        public string? ConfirmationEmailTitle { get; private set; }

        public string? ConfirmationEmailBody { get; private set; }
    }
}
