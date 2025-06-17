using DomainModels;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Storage.DbConfigurations
{
    /// <summary>
    /// Entity Framework configuration fragment that maps the "ChatMessage" entity to the "ChatMessage" database table.
    /// </summary>
    internal class MessageConfiguration : IEntityTypeConfiguration<ChatMessage>
    {
        /// <inheritdoc />
        public void Configure( EntityTypeBuilder<ChatMessage> builder )
        {
            builder.ToTable( nameof(ChatMessage) );

            builder.HasKey( x => x.MessageId );

            builder
                .Property( x => x.Content )
                .IsRequired();

            builder
                .HasOne( p => p.Author )
                .WithMany()
                .HasForeignKey( p => p.AuthorId ).OnDelete( DeleteBehavior.Cascade );

            builder
                .Property( x => x.CreatedTime )
                .IsRequired();
        }
    }
}
