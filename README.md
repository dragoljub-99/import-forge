# ImportForge



ImportForge is a .NET 8 Minimal API that imports speaker products from a CSV file into SQLite using background import jobs. Each upload is validated and either committed to the `Speakers` table or moved to `NeedsFixes` for repair.



## About the project



The solution is split into ImportForge.Api, ImportForge.Domain, and ImportForge.Infrastructure. It uses SQLite via ADO.NET (Microsoft.Data.Sqlite), a streaming CSV parser, and a background worker/queue to process jobs asynchronously. Data and uploads are stored under ./data and ./uploads (configurable via appsettings).

