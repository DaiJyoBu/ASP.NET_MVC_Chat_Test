namespace SecuritySupplements.Contracts
{
    /// <summary>
    /// The interface of the provider of the protection critical data.
    /// </summary>
    public interface ISensitiveDataProvider
    {
        /// <summary>
        /// The database connection string.
        /// </summary>
        string DatabaseConnectionString { get; }

        /// <summary>
        /// The SMTP server host address used to deliver the account activation email.
        /// </summary>
        string ConfirmationEmailSmtpHost { get; }

        /// <summary>
        /// The SMTP server host port used to deliver the account activation email.
        /// </summary>
        int ConfirmationEmailSmtpPort { get; }

        /// <summary>
        /// The email account used as the sender to deliver the account activation email.
        /// </summary>
        string ConfirmationEmailAddress { get; }

        /// <summary>
        /// The email account's password used as the sender to deliver the account activation email.
        /// </summary>
        string ConfirmationEmailPassword { get; }
    }
}
