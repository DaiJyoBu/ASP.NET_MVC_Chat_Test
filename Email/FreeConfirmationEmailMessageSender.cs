using System.Net.Mail;
using System.Net;
using SecuritySupplements.Contracts;
using Microsoft.Extensions.Logging;

namespace Email
{
    /// <summary>
    /// The confirmation email sender implementation tailored to use the free means of delivering the email message.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <param name="sensitiveDataProvider">The provider of the email server connection data.</param>
    /// <param name="confirmationEmailProvider">The provider of the confirmation email title and content.</param>
    internal class FreeConfirmationEmailMessageSender(
        ILoggerFactory loggerFactory, ISensitiveDataProvider sensitiveDataProvider, IConfirmationEmailProvider confirmationEmailProvider ) : IConfirmationEmailSender
    {
        private readonly ILogger _logger = loggerFactory.CreateLogger( nameof( FreeConfirmationEmailMessageSender ) );
        private readonly ISensitiveDataProvider _sensitiveDataProvider = sensitiveDataProvider;
        private readonly IConfirmationEmailProvider _confirmationEmailProvider = confirmationEmailProvider;

        public async Task SendConfirmationAsync( string address, string callbackUrl )
        {
            _logger.Log( LogLevel.Trace, "Creating the network connection." );

            var credential = new NetworkCredential( userName: _sensitiveDataProvider.ConfirmationEmailAddress,
                    password: _sensitiveDataProvider.ConfirmationEmailPassword );
            var client = new SmtpClient( host: _sensitiveDataProvider.ConfirmationEmailSmtpHost,
                port: _sensitiveDataProvider.ConfirmationEmailSmtpPort )
            {
                Credentials = credential,
                EnableSsl = true
            };

            var emailBody = string.Format( _confirmationEmailProvider.ConfirmationEmailBody!, callbackUrl );

            try
            {
                _logger.Log( LogLevel.Trace, "Attempting to send the confirmation email." );

                await client.SendMailAsync( from: _sensitiveDataProvider.ConfirmationEmailAddress,
                    recipients: address,
                    subject: _confirmationEmailProvider.ConfirmationEmailTitle,
                    body: emailBody );

                _logger.Log( LogLevel.Trace, "Confirmation email is sent successfully." );
            }
            catch ( Exception ex )
            {
                _logger.LogError( ex, "Confirmation email is not sent." );
            }

        }

    }
}
