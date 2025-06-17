using Microsoft.EntityFrameworkCore;
using DomainModels;
using Storage.DbConfigurations;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;

namespace Storage
{
    /// <summary>
    /// Database interaction context.
    /// </summary>
    public class ChatDbContext : IdentityDbContext<ChatUser>
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="options">The options to be used by the DbContext.</param>
        public ChatDbContext( DbContextOptions<ChatDbContext> options ) : base( options )
        {
        }

        protected override void OnModelCreating( ModelBuilder modelBuilder )
        {
            base.OnModelCreating( modelBuilder );

            modelBuilder.ApplyConfiguration( new UserConfiguration() );
            modelBuilder.ApplyConfiguration( new MessageConfiguration() );
        }

        public DbSet<ChatMessage> ChatMessages { get; set; }

        public DbSet<ChatUser> ChatUsers { get; set; }

    }
}
