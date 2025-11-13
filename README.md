# BulkInsert

*Modular class library + CLIs for bulk data loading into SQL Server, supporting multiple input file formats and advanced configuration.*

---

### Supported Features

- Input formats: CSV, XLS/XLSX, SAS, XML
- Output: MS SQL Server
- Customizable via appsettings and environment variables (see feature table above)
- Handles schema parsing, column filtering, batching, and error handling

### Design Highlights

- **Extensible Loader Architecture:** Each file type has a dedicated loader class implementing `IBulkLoader`, inheriting shared logic from `BulkLoader`.
- **Factory Pattern:** `BulkLoaderFactory` centralizes loader instantiation, simplifying usage and extension for new formats.
- **Configuration Management:** The `Config` class uses Microsoft.Extensions.Configuration to support layered settings via JSON and environment variables.
- **Event Notifications:** Bulk operations can emit progress and status messages using the `Notifier` event.

---

## BulkInsertClass - Class Library

- `**BulkInsertClass/**`
  - `BulkInsertClass.csproj` – Project file with NuGet dependencies (SQL client, OleDb, CSV reading)
  - Abstract classes:
    - `BulkLoader.cs` – Abstract base class for common bulk loading logic across all concrete input types (SQL operations, column mapping, event notification)
    - `IBulkLoader.cs` – Interface defining the contract for bulk loader implementations
  - Concrete Classes (Interface Implementations):
    - `CSVBulkLoader.cs` – Handles CSV and tab-delimited file loading, schema parsing, and bulk copy to SQL
    - `XLSBulkLoader.cs` – Handles Excel file loading, worksheet management, and bulk copy to SQL (Windows only)
    - `SASBulkLoader.cs` – Handles SAS file loading via OleDb and bulk copy to SQL
    - `XMLBulkLoader.cs` – Handles XML file loading and bulk copy to SQL
  - Factory:
    - `BulkLoaderFactory.cs` – Factory for instantiating the correct bulk loader based on file type (CSV, XLS/XLSX, SAS, XML)
  - `Config.cs` – Static class for config, loading settings from JSON files and environment variables
  - `NotifyEventArgs.cs` – Event argument class for hooking up logging

---

## BulkInsert - load a single file

#### CLI for importing a data file into SQL Server
- supports multiple input formats (CSV, XLS/XLSX, SAS, XML)
- leverages the modular loader architecture from the BulkInsertClass library
- configuration is managed via `App.config` and command-line arguments
- automatically handles file extension overrides, local file copying, and zipped file extraction
- emits progress notifications during bulk operations.

#### `BulkInsertRequest` class
- encapsulates all parameters and logic required to process a bulk data import operation.
- coordinates the end-to-end workflow for loading data into SQL Server
  - configuration mapping
  - input file handling (including zipped files and local copies)
  - instantiates the appropriate loader via the factory
  - schema, batching, column filtering, and logging / event notifications

---

## BatchLoader - load multiple files in parallel

#### CLI for importing multiple data files into SQL Server in parallel
- scans an input folder (optionally recursive) for supported file types
- supports multiple input formats (CSV, XLS/XLSX, SAS, XML)
- uses the `BulkInsertRequest` workflow
- config managed via `App.config` and command-line arguments
- automatically handles file extension overrides and batching
- controls parallelism via the `MaxDOP` setting
- emits progress and error notifications for each file processed

---

## Supported options

| Option              | Required/Optional | Data Type | Default | Applies to File Types | Description                                                                                  |
|----------------------|------------------|-----------|---------|----------------------|----------------------------------------------------------------------------------------------|
| SheetName            | Optional         | string    | None    | xlsx                 | Name of the sheet to load (for xlsx files)                                                   |
| AllowNulls           | Optional         | bool      | False   | all                  | Allow null values in data                                                                    |
| FileExtensionOverride| Optional         | string    | None    | all                  | Override the input file extension, (e.g., `file.txt` is actually csv) format                 |
| Delimiter            | Optional         | string    | ,       | csv                  | Delimiter for csv files                                                    |
| TargetServer         | Required         | string    | None    | all                  | Target SQL instance                                                                          |
| TargetDatabase       | Required         | string    | None    | all                  | Target SQL database                                                                          |
| TargetSchema         | Optional         | string    | dbo     | all                  | Target SQL schema                                                                            |
| TargetTable          | Required         | string    | None    | all                  | Target SQL table                                                                             |
| UseHeader            | Optional         | bool      | True    | csv, xlsx            | Whether to use the first row as column headers                                               |
| HeaderRowsToSkip     | Optional         | int       | 0       | csv, xlsx            | Number of rows to skip from the top                                                          |
| CopyLocal            | Optional         | bool      | False   | all                  | Copy local files to a temp folder for server side processing                                 |
| Overwrite            | Optional         | bool      | False   | all                  | Whether to overwrite existing data (create target table)                                                           |
| Append               | Optional         | bool      | False   | all                  | Whether to append to existing data (target table should already exist)                                                           |
| DefaultColumnWidth   | Optional         | int       | 50      | all                  | Default column width for all columns in the target table                                     |
| BatchSize            | Optional         | int       | 1000    | all                  | Batch size for bulk loading data (number of rows)                                            |
| Comments             | Optional         | string    | None    | all                  | Comments for logging                                                                         |
| SchemaPath           | Optional         | string    | None    | csv                  | Path to the schema file, to define column data types                                         |
| MaxDOP               | Optional         | int       | 1       | all                  | Maximum degree of parallelism                                                                |
| ColumnFilter         | Optional         | string    | None    | all                  | Filter for columns to include in target table                                                |
| QuoteIdentifier      | Optional         | string    | "       | all                  | Character that denotes quote identifiers                                                     |
| EscapeCharacter      | Optional         | string    | \       | all                  | Character that denotes escape quotes                                                         |
| NullValue            | Optional         | string    | None    | all                  | Value that should be translated to null in the target table                                  |
