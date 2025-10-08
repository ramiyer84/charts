package worker

import (
	"context"
	"fmt"
	"io"

	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/dao"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc01"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc02"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc05"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc06"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc10"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc12"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc13"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc19"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc21"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc23"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc24"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc26"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc27"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/inputfile/fc28"
	"github.axa.com/axa-partners-clp/selmed-migration-tool/internal/logger"

	"github.axa.com/axa-partners-clp/mrt-shared/encryption"
)

// ---- Interfaces and deps ----

type Item interface{ Name() string }

type Provider interface {
	List(ctx context.Context) ([]Item, error)
	Open(ctx context.Context, it Item) (io.ReadCloser, error)
}

type Deps struct {
	DB          *dao.DB
	Logger      *logger.Logger
	Encryption  encryption.Client
	BatchID     string
	ImportRange inputfile.ImportRange
}

type Worker struct {
	prov Provider
	deps Deps
}

func New(p Provider, d Deps) *Worker { return &Worker{prov: p, deps: d} }

// ---- Orchestration ----

func (w *Worker) Run(ctx context.Context) error {
	items, err := w.prov.List(ctx)
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	if len(items) == 0 {
		w.deps.Logger.Print("no files to process")
		return nil
	}

	for _, it := range items {
		if err := w.processOne(ctx, it.Name()); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) processOne(ctx context.Context, name string) error {
	// Open content
	rc, err := w.prov.Open(ctx, fileItem(name))
	if err != nil {
		return fmt.Errorf("open %s: %w", name, err)
	}
	defer rc.Close()

	// Per-file transaction
	tx, err := w.deps.DB.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Range cache (used by FC19)
	rng := &inputfile.Range{}

	// Common SourceFile
	sf := &inputfile.SourceFile{
		BatchID:          w.deps.BatchID,
		Filename:         name,
		OriginalFileName: name,
		Encryption:       w.deps.Encryption,
		Db:               w.deps.DB,
		Tx:               tx,
		Logger:           w.deps.Logger,
		Range:            rng,
		Reader:           rc,
	}

	switch route(name) {
	case "FC01":
		f := fc01.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc01: %w", err)
		}
	case "FC02":
		f := fc02.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc02: %w", err)
		}
	case "FC05":
		f := fc05.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc05: %w", err)
		}
	case "FC06":
		f := fc06.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc06: %w", err)
		}
	case "FC10":
		f := fc10.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc10: %w", err)
		}
	case "FC12":
		f := fc12.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc12: %w", err)
		}
	case "FC13":
		f := fc13.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc13: %w", err)
		}
	case "FC19":
		// Validate range once (method returns no error; acts by side-effects)
		rng.Validate(ctx, w.deps.DB, tx, w.deps.BatchID)

		f := fc19.File{SourceFile: sf, Range: rng}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc19: %w", err)
		}
	case "FC21":
		f := fc21.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc21: %w", err)
		}
	case "FC23":
		f := fc23.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc23: %w", err)
		}
	case "FC24":
		f := fc24.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc24: %w", err)
		}
	case "FC26":
		f := fc26.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc26: %w", err)
		}
	case "FC27":
		f := fc27.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc27: %w", err)
		}
	case "FC28":
		f := fc28.File{SourceFile: sf}
		if err := f.ProcessFile(ctx); err != nil {
			return fmt.Errorf("fc28: %w", err)
		}
	default:
		return fmt.Errorf("unknown file type for %q", name)
	}

	// Commit file work
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// helper to adapt name → Item for Open
type fileItem string

func (f fileItem) Name() string { return string(f) }