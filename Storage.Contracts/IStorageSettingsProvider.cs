
namespace Storage.Contracts
{
    /// <summary>
    /// The interface for provider of the settings used by the database service.
    /// </summary>
    public interface IStorageSettingsProvider
    {
        /// <summary>
        /// The frequency of saving bufferized data into the database.
        /// </summary>
        int SavingFrequencyInMilliseconds { get; }

        /// <summary>
        /// The frequency of posting the keep-alive signals into the chat SSE connection between the server and the client javascript handler.
        /// </summary>
        int RefreshingFrequencyInMilliseconds { get; }
    }
}
