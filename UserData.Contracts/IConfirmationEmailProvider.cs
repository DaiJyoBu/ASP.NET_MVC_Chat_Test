
namespace SecuritySupplements.Contracts
{
    /// <summary>
    /// The interface of the provider of the title and the content for the email that is being sent for the new account activation.
    /// </summary>
    public interface IConfirmationEmailProvider
    {
        /// <summary>
        /// Confirmation email title.
        /// </summary>
        string? ConfirmationEmailTitle { get; }

        /// <summary>
        /// Confirmation email content.
        /// </summary>
        string? ConfirmationEmailBody { get; }
    }
}
