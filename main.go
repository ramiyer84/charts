package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"

	// existing internal imports
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/dao"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/logger"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/util"

	// NEW: file provider + worker
	fileprov "github.axa.com/axa-partners-clp/selmed-migration-tool/internal/provider/file"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/worker"
)

const (
	// (kept) your original constant(s)
	applicationName = "github.axa.com/axa-partners-clp/selmed-migration-tool.git"
)

// (kept) your global variables
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

// (kept) any helper functions you had in main.go…

func main() {
	ctx := context.Background()

	// ------------------------------------------------------------
	// (kept) Initialize config defaults, logger, db, tx, etc.
	// ------------------------------------------------------------
	viper.SetDefault("location.input", "input")
	viper.SetDefault("location.processed", "processed")
	viper.SetDefault("location.error", "error")
	viper.SetDefault("source.type", "file") // NEW: default; allows future “s3”

	l = logger.New(applicationName)
	l.Printf("Starting Selmed Migration Tool")

	// (kept) DB initialization – adapt to your dao.InitDB or existing logic
	db, err := dao.InitDB(viper.GetString("db.url"))
	if err != nil {
		l.Fatalf("cannot initialize DB: %v", err)
	}
	defer db.Close()
	database = db

	tx, err := db.Begin()
	if err != nil {
		l.Fatalf("cannot start DB transaction: %v", err)
	}

	// (kept) Encryption / other shared deps setup
	var enc inputfile.EncryptionHandler
	// enc = util.NewEncryption(viper.GetString("encryption.key"))

	// (kept) Batch + optional import range
	batchID := int(time.Now().Unix()) // or your existing batch id logic
	rng := inputfile.ImportRange{
		Start: viper.GetString("import.range.start"),
		End:   viper.GetString("import.range.end"),
	}

	// ------------------------------------------------------------
	// NEW: Build SourceDeps (shared deps for processors)
	// ------------------------------------------------------------
	deps := &inputfile.SourceDeps{
		Db:         db.Sql(), // or db if you already expose *sql.DB
		Tx:         tx,
		Logger:     l,
		Encryption: enc,
		BatchID:    batchID,
		Range:      rng,
	}

	// ------------------------------------------------------------
	// NEW: Choose and build provider (filesystem for now)
	// ------------------------------------------------------------
	sourceType := strings.ToLower(viper.GetString("source.type"))
	var prov interface{}
	switch sourceType {
	case "s3":
		l.Printf("S3 provider not implemented yet; falling back to file provider")
		fallthrough
	default:
		prov = fileprov.Provider{
			Dir:     viper.GetString("location.input"),
			Pattern: "*",
		}
	}

	// ------------------------------------------------------------
	// NEW: Run the worker with the provider
	// ------------------------------------------------------------
	w := worker.New(prov.(fileprov.Provider), deps)

	if err := w.Run(ctx); err != nil {
		l.Fatalf("processing failed: %v", err)
	}

	// ------------------------------------------------------------
	// (kept) Commit/rollback and final log
	// ------------------------------------------------------------
	if err := tx.Commit(); err != nil {
		l.Fatalf("DB commit failed: %v", err)
	}
	l.Printf("Selmed Migration Tool completed successfully")
}