# ASP.NET Core MVC Chat Application

A secure, modular, and scalable chat application built with ASP.NET Core MVC. This project demonstrates core concepts of modern .NET web application development, including MVC architecture, layered design, dependency injection, and basic security practices — including storing the secrets in the Hashicorp Vault.

## 🚀 Features

- 🧑‍🤝‍🧑 Real-time chat interface
- 🔐 User authentication & authorization
- 📧 Email service integration
- 📁 Modular architecture with separate data, domain, and storage layers
- 🗂️ Use of contracts for abstractions and loose coupling
- 🌐 Razor-based UI following MVC pattern
- 🔒 SSL/TLS-ready configuration (secure by default)
- 🛠️ App settings separated by environment (Development, Production)
- 📦 Dependency injection for all services

---

## 📁 Project Structure

├── ASP.Net_Core_MVC_Chat_Test.sln # Solution file
├── DomainModels # Entity models
├── Email # Email service logic
├── MVC_SSL_Chat/ # Main ASP.NET Core MVC web app
│ ├── Controllers
│ ├── Views
│ ├── Models
│ ├── Areas
│ ├── Program.cs
│ ├── appsettings.json
├── SecuritySupplements # Additional security-related logic
├── Storage # Data storage implementation
├── Storage.Contracts # Interfaces for data storage
├── UserData.Contracts # Interfaces for user-related logic

---

## 🧪 Getting Started

### Prerequisites

- [.NET 6+ SDK](https://dotnet.microsoft.com/download)
- SQL Server (or localdb) if using database features
- Visual Studio or VS Code
- PostgreSQL
- Hashicorp Vault

### Running the App

1. Clone the repository:
   ```bash
   git clone https://github.com/DaiJyoBu/ASP.NET_MVC_Chat_Test
2. Navigate to the main project folder:
   cd ASP.NET_MVC_Chat_Test-main/MVC_SSL_Chat
3. Restore dependencies and build:
   dotnet restore
   dotnet build
4. Prepare the email account – it will be used for sending the account creation confirmation messages.
5. Prepare the PostgreSQL – the database must be ready, the migrations from the “/ASP.Net_Core_MVC_Chat_Test/Storage/Migrations” must be applied.
6. Prepare the Hashicorp Vault – it must contain the "DatabaseConnection" (the PostgreSQL database connection string), "EmailSmtpHost", "EmailSmtpPort", "EmailAddress" and "EmailPassword" (the email account credentials) secrets
7.  Prepare the Hashicorp Vault credentials – they must be stored either as the User Secrets’ "HashicorpVault" section “Token”, “Address”, “Path” and “MountPoint” keys (when built using the Debug configuration) or the machine’s “HashicorpVaultCredentialToken”, “HashicorpVaultCredentialAddress”, “HashicorpVaultCredentialPath” and “HashicorpVaultCredentialMountPoint” (when built using the Release configuration)
8. Run the application:
   dotnet run
9. Open in browser:
   https://localhost:5001

🔧 Configuration
Edit appsettings.json or appsettings.Development.json to set connection strings, email provider configs, or other app-level settings.

🧩 Technologies Used
    • ASP.NET Core MVC
    • Razor Views
    • C# 10
    • Dependency Injection
    • Layered Architecture
    • Email Integration
    • SQL Server or EF Core (optional)
    • SSL-ready Configuration

📌 TODO / Improvements
    • Add unit & integration tests
    • Deploy to Azure or another cloud provider
    • Add SignalR for real-time communication
    • Add Docker support

📄 License
MIT License. See LICENSE file for details.

👤 Author
Sergey Korolkov
[LinkedIn](https://www.linkedin.com/in/sergey-korolkov-518508a4/) • [GitHub](https://github.com/DaiJyoBu)
