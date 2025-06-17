
namespace SecuritySupplements.HashicorpVault
{
    /// <summary>
    /// The wrapper for the Hashicorp Vault access credentials.
    /// </summary>
    public class VaultCredentials
    {
        /// <summary>
        /// Hashicorp Vault access Token.
        /// </summary>
        public string? Token { get; set; }

        /// <summary>
        /// Hashicorp Vault address.
        /// </summary>
        public string? Address { get; set; }

        /// <summary>
        /// The path to the secret within the Hashicorp Vault.
        /// </summary>
        public string? Path { get; set; }

        /// <summary>
        /// Hashicorp Vault - the mount point for the KeyValue backend.
        /// </summary>
        public string? MountPoint { get; set; }

    }
}
