using Microsoft.AspNetCore.Identity;

namespace DomainModels
{
    /// <summary>
    /// The user of the chat.
    /// </summary>
    public class ChatUser : IdentityUser
    {
        /// <summary>
        /// The user's registration date.
        /// </summary>
        public DateTime RegistrationDate { get; set; }
    }
}
