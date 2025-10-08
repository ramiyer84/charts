package worker

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/dao"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/logger"

	// FC processors
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc01"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc02"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc05"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc06"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc10"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc12"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc18"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc19"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc21"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc23"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc24"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc26"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc28"

	// shared encryption client
	"github.axa.com/axa-partners-clp/mrt-shared/encryption"
)

//
// Interfaces (kept where used, per Igor)
//

type Item interface{ Name() string }

// Provider lists items and opens readers for them (fs/S3/etc.).
type Provider interface {
	List(ctx context.Context) ([]string, error)
	Open(ctx context.Context, name string) (io.ReadCloser, error)
	OnSuccess(ctx context.Context, name string) error
	OnError(ctx context.Context, name string, cause error) error
}

//
// Concrete dependencies the worker needs
//

type Deps struct {
	DB          *dao.DB
	Logger      *logger.Logger
	Encryption  encryption.Client
	BatchID     string
	ImportRange inputfile.ImportRange // lightweight Start/End window (from config)
}

//
// Worker wiring
//

type Worker struct {
	prov Provider
	deps Deps
}

func New(prov Provider, deps Deps) *Worker {
	return &Worker{prov: prov, deps: deps}
}

//
// Orchestration
//

func (w *Worker) Run(ctx context.Context) error {
	names, err := w.prov.List(ctx)
	if err != nil {
		return fmt.Errorf("list input items: %w", err)
	}

	for _, name := range names {
		if err := w.processOne(ctx, name); err != nil {
			_ = w.prov.OnError(ctx, name, err) // best-effort
			return err
		}
		if e := w.prov.OnSuccess(ctx, name); e != nil {
			w.deps.Logger.Printf("post-success action failed for %s: %v", name, e)
		}
	}

	return nil
}

func (w *Worker) processOne(ctx context.Context, name string) error {
	// Open content from provider
	rc, err := w.prov.Open(ctx, name)
	if err != nil {
		return fmt.Errorf("open %s: %w", name, err)
	}
	defer rc.Close()

	// Per-file transaction
	tx, err := w.deps.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx for %s: %w", name, err)
	}

	// Common per-file context passed to FC processors
	sf := &inputfile.SourceFile{
		Db:          w.deps.DB,
		Tx:          tx,
		Logger:      w.deps.Logger,
		Encryption:  w.deps.Encryption,
		BatchID:     w.deps.BatchID,
		Filename:    name,
		Reader:      rc,
		ImportRange: w.deps.ImportRange,
	}

	// Route to the correct FC processor
	switch route(name) {
	case "FC01":
		f := fc01.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc01: %w", err)
		}
	case "FC02":
		f := fc02.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc02: %w", err)
		}
	case "FC05":
		f := fc05.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc05: %w", err)
		}
	case "FC06":
		f := fc06.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc06: %w", err)
		}
	case "FC10":
		f := fc10.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc10: %w", err)
		}
	case "FC12":
		f := fc12.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc12: %w", err)
		}
	case "FC18":
		f := fc18.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc18: %w", err)
		}
	case "FC19":
		// FC19 ONLY: build & validate runtime cache and inject it
		rng := &inputfile.Range{}
		if err := rng.Validate(ctx, w.deps.DB, tx, w.deps.BatchID); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("fc19: range validate failed: %w", err)
		}
		f := fc19.File{
			SourceFile: sf,
			Range:      rng,
		}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc19: %w", err)
		}
	case "FC21":
		f := fc21.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc21: %w", err)
		}
	case "FC23":
		f := fc23.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc23: %w", err)
		}
	case "FC24":
		f := fc24.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc24: %w", err)
		}
	case "FC26":
		f := fc26.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc26: %w", err)
		}
	case "FC28":
		f := fc28.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			_ = tx.Rollback(); return fmt.Errorf("fc28: %w", err)
		}
	default:
		_ = tx.Rollback()
		return fmt.Errorf("unsupported file type for %s", name)
	}

	// Commit per-file transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", name, err)
	}
	return nil
}

//
// If you already have this in internal/worker/route.go, delete this copy.
//

func route(name string) string {
	for i := 0; i < len(name); i++ {
		if name[i] == '_' || name[i] == '.' {
			return name[:i]
		}
	}
	return name
}