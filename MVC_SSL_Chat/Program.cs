using MVC_SSL_Chat.Internal;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting.Internal;
using Storage;
using SecuritySupplements.Contracts;
using SecuritySupplements;
using SecuritySupplements.HashicorpVault;
using System.Reflection;
using NLog.Extensions.Logging;

internal class Program
{
    public static async Task Main( string[] args )
    {
        var builder = WebApplication.CreateBuilder( args );

        builder.Services.AddLogging( loggingBuilder =>
                     {
                         loggingBuilder.ClearProviders();
                         loggingBuilder.SetMinimumLevel( LogLevel.Trace );

                         var nLogLoggingConfiguration =
                             new NLogLoggingConfiguration(
                                 builder.Configuration.GetSection( "NLog" )
                             );

                         loggingBuilder.AddNLog( nLogLoggingConfiguration );
                     } );

#pragma warning disable ASP0000 // Suppress the warning - getting the logging service before the build is necessary to log the information from the Hashicorp Vault communication
        var loggerFactory = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<ILoggerFactory>();
#pragma warning restore ASP0000

        var rootLogger = loggerFactory.CreateLogger<Program>();

        var nonSensitiveDataProvider = new NonSensitiveSettingsProvider( loggerFactory );
        nonSensitiveDataProvider.ReadAppConfig( builder.Configuration );

        var vaultProvider = new HashicorpProvider( loggerFactory, nonSensitiveDataProvider );
        VaultCredentials? vaultCredentials = null;

        VaultCredentialsReader vaultCredentialsReader = new VaultCredentialsReader( loggerFactory );

        if( !builder.Environment.IsDevelopment() )
        {
            vaultCredentials = vaultCredentialsReader.ReadFromEnvironmentVariables();
        }
        else
        {
            vaultCredentials = vaultCredentialsReader.ReadFromUserSecrets( builder.Configuration );
        }

        if( vaultCredentials == null )
        {
            rootLogger.LogCritical( "Error reading credentials! Application shutdown." );
            Environment.Exit( 1 );
        }

        await vaultProvider.LoadVaultData( vaultCredentials );

        if( !vaultProvider.IsRead )
        {
            rootLogger.LogCritical( "Error accessing the sensitive data! Application shutdown." );
            Environment.Exit( 1 );
        }

        // Add services to the container.
        builder.Services.AddControllersWithViews();
        builder.Services.AddImplementations( vaultProvider, nonSensitiveDataProvider );

        var app = builder.Build();

        // Configure the HTTP request pipeline.
        if(!app.Environment.IsDevelopment())
        {

            app.UseExceptionHandler( "/Home/Error" );
            // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
            app.UseHsts();
        }
        else
        {
        }
        //var temp = app.Services.GetService( typeof( SecuritySupplements.Contracts.ISensitiveDataProvider ) );
        app.UseHttpsRedirection();
        app.UseStaticFiles();

        app.UseRouting();

        app.UseAuthorization();

        app.MapControllerRoute(
            name: "default",
            pattern: "{controller=Home}/{action=Index}/{id?}" );

        app.MapRazorPages();

        app.Run();
    }
}