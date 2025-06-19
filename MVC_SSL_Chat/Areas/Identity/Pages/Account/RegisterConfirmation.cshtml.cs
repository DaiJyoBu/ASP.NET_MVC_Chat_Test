// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
#nullable disable

using System;
using System.Text;
using System.Threading.Tasks;
using DomainModels;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.UI.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.WebUtilities;

namespace MVC_SSL_Chat.Areas.Identity.Pages.Account
{
    /// <summary>
    /// RegisterConfirmation page's code behind implementation - technically identical to the default.
    /// </summary>
    [AllowAnonymous]
    public class RegisterConfirmationModel( UserManager<ChatUser> userManager ) : PageModel
    {
        private readonly UserManager<ChatUser> _userManager = userManager;

        /// <summary>
        ///     This API supports the ASP.NET Core Identity default UI infrastructure and is not intended to be used
        ///     directly from your code. This API may change or be removed in future releases.
        /// </summary>
        public string Email { get; set; }

        /// <summary>
        ///     This API supports the ASP.NET Core Identity default UI infrastructure and is not intended to be used
        ///     directly from your code. This API may change or be removed in future releases.
        /// </summary>
        public bool DisplayConfirmAccountLink { get; set; }

        /// <summary>
        ///     This API supports the ASP.NET Core Identity default UI infrastructure and is not intended to be used
        ///     directly from your code. This API may change or be removed in future releases.
        /// </summary>
        public string EmailConfirmationUrl { get; set; }

        public async Task<IActionResult> OnGetAsync( string email, string returnUrl = null )
        {
            if ( email == null )
            {
                return RedirectToPage( "/Index" );
            }
            returnUrl = returnUrl ?? Url.Content( "~/" );

            var user = await _userManager.FindByEmailAsync( email );
            if ( user == null )
            {
                return NotFound( $"Unable to load user with email '{ email }'." );
            }

            Email = email;

            return Page();
        }
    }
}
