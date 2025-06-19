using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

using MVC_SSL_Chat.Internal.Interfaces;
using MVC_SSL_Chat.Models;
using Storage.Contracts;
using MVC_SSL_Chat.Internal;
using DomainModels;
using Microsoft.AspNetCore.Identity;

namespace MVC_SSL_Chat.Controllers
{
    /// <summary>
    /// The main controller of the web site.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <param name="userManager">The user manager.</param>
    /// <param name="bufferingService">The buffering service.</param>
    /// <param name="dbService">The service for the database interaction.</param>
    /// <param name="storageSettingsProvider">The provider of the storage settings.</param>
    /// <param name="chatMessageDispatcher">The dispatcher of the messages sent while the application is working.</param>
    /// <param name="clock">The clock used for the dates.</param>
    [Route("chat") ]
    public class ChatController( ILoggerFactory loggerFactory,
        UserManager<ChatUser> userManager,
        IBufferingService bufferingService, 
        IDbService dbService, 
        IStorageSettingsProvider storageSettingsProvider,
        IChatMessageDispatcher chatMessageDispatcher,
        IClock clock ) : Controller
    {
        private readonly ILogger _logger = loggerFactory.CreateLogger( nameof( ChatController ) );

        private readonly UserManager<ChatUser> _userManager = userManager;

        private readonly IBufferingService _bufferingService = bufferingService;

        private readonly IChatMessageDispatcher _chatMessageDispatcher = chatMessageDispatcher;
        
        private readonly IDbService _dbService = dbService;

        private readonly IStorageSettingsProvider _storageSettingsProvider = storageSettingsProvider;

        private readonly IClock _clock = clock;

        /// <summary>
        /// The endpoint to be used by the chat page's javascript code to establish the receiving of the messages 
        /// as they are being posted by the users. Not supposed to be called from the browser directly.
        /// </summary>
        /// <returns></returns>
        [ HttpGet("stream") ]
        public async Task Stream()
        {
            _logger.LogTrace( "Start message streaming." );

            Response.Headers[ "Content-Type" ] = "text/event-stream";
            Response.Headers[ "Cache-Control" ] = "no-cache";
            Response.Headers[ "Connection" ] = "keep-alive";

            // The sending method is registered once per controller.
            _chatMessageDispatcher.Register( SendToClient );

            SendMessagesPerLastDay();

            _logger.LogTrace( "The message streaming is started." );
            try
            {
                var clientDisconnectedToken = HttpContext.RequestAborted;

                // Keep the connection alive
                while ( !clientDisconnectedToken.IsCancellationRequested )
                {
                    await Response.WriteAsync( ": keep-alive\n\n" );
                    await Response.Body.FlushAsync();
                    await Task.Delay( _storageSettingsProvider.RefreshingFrequencyInMilliseconds, clientDisconnectedToken ); // every 15 seconds
                }
            }
            catch (TaskCanceledException)
            {
                _logger.LogTrace( "Client disconnected. Stream halted." );
            }
            finally
            {
                _chatMessageDispatcher.Unregister( SendToClient );
            }
            _logger.LogTrace( "The message streaming is finished." );
        }

        private async void SendMessagesPerLastDay()
        {
            _logger.LogTrace( "Sending the chat messages per the last day." );
            try
            {
                var to = _clock.UtcNow;
                var from = to.AddDays( -1 );
                var rawMessages = await _dbService.GetMessages( from, to );

                foreach(var rawMessage in rawMessages)
                {
                    SendToClient( rawMessage.ToRequestModel() );
                }
                _logger.LogTrace( "The chat messages per the last day were sent successfully." );
            }
            catch( Exception ex )
            {
                _logger.LogError( ex, "Failed sending the chat messages per the last day." );
                throw;
            }
        }

        /// <summary>
        /// The method that makes sure that the message sent by one client is received by all active clients.
        /// </summary>
        /// <param name="message">The new message sent by one of the clients.</param>
        private void SendToClient( SendMessageRequestModel message )
        {
            _logger.LogTrace( "Sending the chat message via the stream." );
            try
            {
                var json = JsonSerializer.Serialize( message );
                var formatted = $"data: {json}\n\n";
                Response.WriteAsync( formatted ).Wait();
                Response.Body.FlushAsync().Wait();
                _logger.LogTrace( "The chat message is sent via the stream." );
            }
            catch( Exception ex )
            {
                _logger.LogError( ex, "Failed sending the chat message via the stream." );
                throw;
            }
        }

        /// <summary>
        /// The post method to be called by the javascript code when the current user sends the message
        /// </summary>
        /// <param name="receivedShell">The new message sent by one of the clients.</param>
        /// <returns></returns>
        [HttpPost("send") ]
        public async Task<IActionResult> SendAsync( [FromBody] SendMessageRequestModel receivedShell )
        {
            _logger.LogTrace( "Receiving the chat message from the stream." );

            try
            {
                var user = await _userManager.GetUserAsync( User );

                var dateTime = _clock.UtcNow;
                receivedShell.CreatedTime = dateTime;
                receivedShell.AuthorName = user!.UserName;
                _chatMessageDispatcher.Dispatch( receivedShell );

                _bufferingService.Append( receivedShell.ToMessageModel( user! ) );
                
                _logger.LogTrace( "The received chat message is handled successfully." );
                
                return Ok();
            }
            catch( Exception ex )
            {
                _logger.LogError( ex, "Failed handling the chat message received from the stream." );
                throw;
            }
        }
    }
}
