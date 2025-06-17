using Microsoft.Extensions.DependencyInjection;
using SecuritySupplements.Contracts;

namespace Email
{
    /// <summary>
    /// Extension for '<see cref="IServiceCollection"/>' (Microsoft.Extensions.DependencyInjection)
    /// </summary>
    public static class Module
    {
        /// <summary>
        /// Add email implementations to DI.
        /// </summary>
        /// <param name="serviceCollection">Services.</param>
        /// <returns></returns>
        public static IServiceCollection AddEmailImplementations( this IServiceCollection serviceCollection )
        {
            serviceCollection.AddSingleton<IConfirmationEmailSender, FreeConfirmationEmailMessageSender>();
            serviceCollection.AddSingleton<IConfirmationEmailProvider, ConfirmationEmailProvider>();
            return serviceCollection;
        }
    }
}
