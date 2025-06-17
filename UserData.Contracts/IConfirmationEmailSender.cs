namespace SecuritySupplements.Contracts
{
    /// <summary>
    /// The sender of the email that is being sent for the new account activation.
    /// </summary>
    public interface IConfirmationEmailSender
    {
        /// <summary>
        /// Send the email to the address of the newly registered user.
        /// </summary>
        /// <param name="address">Newly registered user's address.</param>
        /// <param name="callbackUrl">The URL to be parsed into the confirmation email's body that will allow user to activate his or her account.</param>
        Task SendConfirmationAsync( string address, string callbackUrl );
    }
}
