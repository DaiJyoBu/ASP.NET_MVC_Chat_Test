using DomainModels;
using MVC_SSL_Chat.Models;

namespace MVC_SSL_Chat.Internal
{
    /// <summary>
    /// The helper class for converting the <see cref="MessageModel"/>, <see cref="UserModel"/>, <see cref="ChatMessage"/>, 
    /// <see cref="ChatUser"/> and <see cref="SendMessageRequestModel" /> into each other.
    /// </summary>
    public static class MvcDataConverterHelper
    {
        /// <summary>
        /// Converts the <see cref="ChatMessage" /> object into the <see cref="SendMessageRequestModel" /> object.
        /// </summary>
        /// <param name="chatMessage">The original <see cref="ChatMessage" /> object.</param>
        /// <returns>The object converted to <see cref="SendMessageRequestModel" />.</returns>
        public static SendMessageRequestModel ToRequestModel( this ChatMessage chatMessage ) =>
            new SendMessageRequestModel()
            {
                AuthorName = chatMessage.Author!.UserName,
                Content = chatMessage.Content,
                CreatedTime = chatMessage.CreatedTime
            };

        /// <summary>
        /// Converts the <see cref="SendMessageRequestModel" /> object to the <see cref="MessageModel" /> object.
        /// </summary>
        /// <param name="sendMessageRequestModel">The original object.</param>
        /// <returns>The converted object.</returns>
        public static MessageModel ToMessageModel( this SendMessageRequestModel sendMessageRequestModel, ChatUser author )
        {
            var dateTime = sendMessageRequestModel.CreatedTime!.Value;

            return new MessageModel
            {
                Content = sendMessageRequestModel.Content!,
                CreatedTime = dateTime,
                Author = author
            };
        }

        /// <summary>
        /// Converts the <see cref="ChatMessage" /> object into the <see cref="MessageModel" /> object.
        /// </summary>
        /// <param name="message">The original <see cref="ChatMessage" /> object.</param>
        /// <returns>The object converted to <see cref="MessageModel" />.</returns>
        public static ChatMessage ToDatabaseEntity( this MessageModel message )
        {
            return new ChatMessage
            {
                MessageId = message.MessageId,
                Content = message.Content,
                CreatedTime = message.CreatedTime,
                AuthorId = message.Author.Id,
                Author = message.Author
            };
        }
    }
}
