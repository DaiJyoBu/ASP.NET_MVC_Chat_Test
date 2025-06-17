namespace DomainModels
{
    /// <summary>
    /// The message sent by the chat user.
    /// </summary>
    public class ChatMessage
    {
        /// <summary>
        /// The primary database key for the message.
        /// </summary>
        public Guid MessageId { get; set; }

        /// <summary>
        /// The primary key for the user who has sent the message.
        /// </summary>
        public string AuthorId { get; set; } = string.Empty;

        /// <summary>
        /// The text content of the message.
        /// </summary>
        public string Content { get; set; } = string.Empty;

        /// <summary>
        /// The creation time of the message.
        /// </summary>
        public DateTime CreatedTime { get; set; }

        /// <summary>
        /// The loaded user who has sent the message.
        /// </summary>
        public ChatUser? Author { get; set; }
    }
}
