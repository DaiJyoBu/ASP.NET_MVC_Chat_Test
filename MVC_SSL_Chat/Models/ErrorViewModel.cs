namespace MVC_SSL_Chat.Models
{
    /// <summary>
    /// The view model that represents the error happened in the application.
    /// </summary>
    public class ErrorViewModel
    {
        /// <summary>
        /// The request id.
        /// </summary>
        public string? RequestId { get; set; }

        /// <summary>
        /// The indicator that the request id is not null.
        /// </summary>
        public bool ShowRequestId => !string.IsNullOrEmpty( RequestId );
    }
}
