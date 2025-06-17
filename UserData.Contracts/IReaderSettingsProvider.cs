
namespace SecuritySupplements.Contracts
{
    /// <summary>
    /// The provider of the settings used by the sensitive data provider.
    /// </summary>
    public interface IReaderSettingsProvider
    {
        /// <summary>
        /// The maximum amount of sensitive data access attempts.
        /// </summary>
        int ReadingAttemptsMax { get; }

        /// <summary>
        /// The timeout to abide after each unsuccessful sensitive data access attempt.
        /// </summary>
        int ReadingDelay { get; }
    }
}
