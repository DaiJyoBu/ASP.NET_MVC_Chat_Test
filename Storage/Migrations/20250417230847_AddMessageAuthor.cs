using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Storage.Migrations
{
    /// <inheritdoc />
    public partial class AddMessageAuthor : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.RenameColumn(
                name: "CreatedDate",
                table: "ChatMessage",
                newName: "CreatedTime");

            migrationBuilder.AddColumn<Guid>(
                name: "AuthorId",
                table: "ChatMessage",
                type: "uuid",
                nullable: false,
                defaultValue: new Guid("00000000-0000-0000-0000-000000000000"));

            migrationBuilder.CreateIndex(
                name: "IX_ChatMessage_AuthorId",
                table: "ChatMessage",
                column: "AuthorId");

            migrationBuilder.AddForeignKey(
                name: "FK_ChatMessage_ChatUser_AuthorId",
                table: "ChatMessage",
                column: "AuthorId",
                principalTable: "ChatUser",
                principalColumn: "UserId",
                onDelete: ReferentialAction.Cascade);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_ChatMessage_ChatUser_AuthorId",
                table: "ChatMessage");

            migrationBuilder.DropIndex(
                name: "IX_ChatMessage_AuthorId",
                table: "ChatMessage");

            migrationBuilder.DropColumn(
                name: "AuthorId",
                table: "ChatMessage");

            migrationBuilder.RenameColumn(
                name: "CreatedTime",
                table: "ChatMessage",
                newName: "CreatedDate");
        }
    }
}
