package main

import (
    "context"
    "fmt"
    "time"

    "github.axa.com/axa-partners-clp/selmed-migration-tool/internal/dao"
    "github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile"
    "github.axa.com/axa-partners-clp/selmed-migration-tool/internal/logger"
    "github.axa.com/axa-partners-clp/selmed-migration-tool/internal/provider"
    "github.axa.com/axa-partners-clp/selmed-migration-tool/internal/worker"
    "github.com/spf13/viper"
)

const (
    applicationName = "github.axa.com/axa-partners-clp/selmed-migration-tool.git"
)

var (
    l                    *logger.Logger
    database             *dao.DB
    Debug                bool
    importDateRangeStart *time.Time
    importDateRangeEnd   *time.Time
    shouldImportInRange  bool
    fakeCommentsCount    = 3
    fakeCommentIndex     int
)

func main() {
    // Context for cancellation, deadlines etc.
    ctx := context.Background()

    // --- Initialize configuration defaults ---
    viper.SetDefault("location.input", "input")
    viper.SetDefault("location.processed", "processed")
    viper.SetDefault("location.error", "error")
    viper.SetDefault("source.type", "file") // future: could be "s3"

    // --- Create logger ---
    l = logger.New(applicationName)
    l.Printf("Starting Selmed Migration Tool")

    // --- Read configuration / environment ---
    secretKey := viper.GetString("encryption.key")
    if secretKey == "" {
        panic("missing encryption.key in configuration")
    }

    enc := inputfile.NewEncryptionHandler(secretKey)

    // --- Initialize DB connection ---
    dbClient := dao.MustCreate(viper.GetString("database.url"))
    dbClient.Username(viper.GetString("database.username"))
    dbClient.Password(viper.GetString("database.password"))
    dbClient.Debug(Debug)

    database = dao.CreateClient(dbClient, l)
    defer database.Close()

    tx, err := database.Begin()
    if err != nil {
        panic(fmt.Sprintf("cannot start DB transaction: %v", err))
    }

    // --- Prepare dependencies for processing ---
    deps := provider.SourceDeps{
        Logger:     l,
        DB:         database,
        Tx:         tx,
        Encryption: enc,
        Range: inputfile.ImportRange{
            Start: viper.GetString("import.range.start"),
            End:   viper.GetString("import.range.end"),
        },
    }

    // --- Initialize the provider (file-based for now) ---
    fileProv := provider.NewFileProvider(
        viper.GetString("location.input"),
        viper.GetString("location.processed"),
        viper.GetString("location.error"),
    )

    // --- Run the worker with the provider ---
    w := worker.New(fileProv, deps)

    if err := w.Run(ctx); err != nil {
        panic("processing failed: " + err.Error())
    }

    if err := tx.Commit(); err != nil {
        panic("DB commit failed: " + err.Error())
    }

    l.Printf("Selmed Migration Tool completed successfully")
}