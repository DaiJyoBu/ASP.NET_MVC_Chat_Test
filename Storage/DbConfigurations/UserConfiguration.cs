using DomainModels;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Storage.DbConfigurations
{
    /// <summary>
    /// Entity Framework configuration fragment that maps the "ChatUser" entity to the "AspNetUsers" database table.
    /// </summary>
    internal class UserConfiguration : IEntityTypeConfiguration<ChatUser>
    {
        /// <inheritdoc />
        public void Configure( EntityTypeBuilder<ChatUser> builder )
        {
            builder.ToTable( "AspNetUsers" );

            builder.HasKey( x => x.Id );

            builder
                .Property( x => x.UserName )
                .IsRequired();

            builder
                .Property( x => x.RegistrationDate )
                .IsRequired();
        }
    }
}
