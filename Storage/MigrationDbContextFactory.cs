using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace Storage
{
    /// <summary>
    /// DbContext migration creation factory
    /// </summary>
    internal class MigrationDbContextFactory : IDesignTimeDbContextFactory<ChatDbContext>
    {
        /// <inheritdoc />
        public ChatDbContext CreateDbContext( string[] args )
        {
            IConfiguration configuration = new ConfigurationBuilder()
                .AddJsonFile( "appsettings.json" )
                .Build();

            var connectionString = configuration[ "ConnectionStrings:DefaultConnection" ];

            var optionsBuilder = new DbContextOptionsBuilder<ChatDbContext>();
            optionsBuilder.UseNpgsql( connectionString );

            return new ChatDbContext( optionsBuilder.Options );
        }
    }
}
