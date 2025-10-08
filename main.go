package main

import (
	"context"
	"github.axa.com/axa-partners-clp/mrt-shared/encryption"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/dao"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/logger"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/provider"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/worker"
	"github.com/spf13/viper"
	"strconv"
	"time"
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
	viper.SetDefault("location.pattern", "*")

	l = logger.Create(applicationName)
	l.Printf("Starting Selmed Migration Tool")

	// --- Read configuration / environment ---
	secretKey := viper.GetString("encryption.key")
	if secretKey == "" {
		panic("missing encryption.key in configuration")
	}

	enc := encryption.New(secretKey, nil)

	// --- Initialize DB connection ---
	dbClient := dao.MustCreate(viper.GetString("database.url"))
	dbClient.Username(viper.GetString("database.username"))
	dbClient.Password(viper.GetString("database.password"))
	dbClient.Debug(Debug)

	database = dao.CreateClient(dbClient, l)
	defer database.Close()

	batchID := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	rngPtr := inputfile.ImportRange{
		Start: viper.GetString("import.range.start"),
		End:   viper.GetString("import.range.end"),
	}
	// --- Prepare dependencies for processing ---
	deps := worker.Deps{
		Logger:     l,
		DB:         database,
		Encryption: enc,
		BatchID:    batchID,
		Range:      rngPtr,
	}

	// Provider from config (worker owns the interface)
	prov := provider.NewFileProvider(
		viper.GetString("location.input"),
		viper.GetString("location.pattern"),
	)

	// Run (worker handles per-file transactions)
	w := worker.New(prov, deps)
	if err := w.Run(ctx); err != nil {
		panic("processing failed: " + err.Error())
	}
	l.Printf("Selmed Migration Tool completed successfully")
}
