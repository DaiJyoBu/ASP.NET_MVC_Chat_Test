namespace MVC_SSL_Chat.Models
{
    /// <summary>
    /// The model for wrapping the data from the javascript code.
    /// </summary>
    public class SendMessageRequestModel
    {
        public string? AuthorName { get; set; }
        public string? Content { get; set; }
        public DateTime? CreatedTime { get; set; }
    }
}
