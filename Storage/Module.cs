using DomainModels;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Storage.Contracts;
using SecuritySupplements.Contracts;

namespace Storage
{
    /// <summary>
    /// Extension for '<see cref="IServiceCollection"/>' (Microsoft.Extensions.DependencyInjection)
    /// </summary>
    public static class Module
    {
        /// <summary>
        /// Add storage implementations to DI.
        /// </summary>
        /// <param name="serviceCollection">Services.</param>
        /// <param name="settingsProvider">The implementation of the sensitive data provider interface.</param>
        /// <returns></returns>
        public static IServiceCollection AddStorageImplementations( 
            this IServiceCollection serviceCollection, ISensitiveDataProvider sensitiveDataProvider )
        {
            serviceCollection.AddDbContext<ChatDbContext>(
                options => options.UseNpgsql( sensitiveDataProvider.DatabaseConnectionString ) );
            serviceCollection.AddDefaultIdentity<ChatUser>( 
                options => options.SignIn.RequireConfirmedAccount = true ).AddEntityFrameworkStores<ChatDbContext>();
            serviceCollection.AddSingleton<IChatDbContextFactory, ChatDbContextFactory>();
            serviceCollection.AddSingleton<IDbService, DbService>();

            return serviceCollection;
        }
    }
}
