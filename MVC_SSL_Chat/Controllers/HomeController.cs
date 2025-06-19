using Microsoft.AspNetCore.Mvc;
using MVC_SSL_Chat.Models;
using System.Diagnostics;

namespace MVC_SSL_Chat.Controllers
{
    /// <summary>
    /// The default controller of the website.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    public class HomeController( ILoggerFactory loggerFactory ) : Controller
    {
        private readonly ILogger _logger = loggerFactory.CreateLogger( nameof( HomeController ) );

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Index()
        {
            _logger.LogTrace( "The index page is requested" );
            try
            {
                return View();
            }
            catch ( Exception ex )
            {
                _logger.LogError( ex, "Error while providing the index page" );
                throw;
            }
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            _logger.LogTrace( "The error page is requested" );
            try
            {
                return View( new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier } );
            }
            catch( Exception ex )
            {
                _logger.LogError( ex, "Error while providing the error page" );
                throw;
            }
        }
    }
}
