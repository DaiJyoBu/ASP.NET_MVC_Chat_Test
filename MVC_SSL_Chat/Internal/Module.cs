using MVC_SSL_Chat.Internal.Implementations;
using MVC_SSL_Chat.Internal.Interfaces;
using Email;
using Storage;
using SecuritySupplements;
using SecuritySupplements.Contracts;
using Storage.Contracts;

namespace MVC_SSL_Chat.Internal
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
        /// <param name="hashicorpProvider">The implementation of the sensitive data provider interface.</param>
        /// <param name="nonSensitiveDataProvider">The implementation of the non-sensitive data provider interface.</param>
        /// <returns></returns>
        public static IServiceCollection AddImplementations( this IServiceCollection serviceCollection, 
            HashicorpProvider hashicorpProvider, 
            NonSensitiveSettingsProvider nonSensitiveDataProvider )
        {
            serviceCollection.AddSingleton<IBufferingService, BufferingService>();
            serviceCollection.AddSingleton<IChatMessageDispatcher, ChatMessageDispatcher>();
            serviceCollection.AddSingleton<IClock, SystemClock>();
            serviceCollection.AddSingleton<ISensitiveDataProvider>( hashicorpProvider );
            serviceCollection.AddSingleton<IStorageSettingsProvider>( nonSensitiveDataProvider );
            serviceCollection.AddSingleton<IReaderSettingsProvider>( nonSensitiveDataProvider );

            return serviceCollection
                .AddStorageImplementations( hashicorpProvider )
                .AddEmailImplementations();
        }
    }
}
