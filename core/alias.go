package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/theleeeo/laika/core/resource"
)

// AliasBackend is the narrow alias-administration surface that read-alias
// convergence needs. *elasticsearch.Client implements it.
type AliasBackend interface {
	// GetAlias returns the concrete index the alias points to, or "" if the
	// alias does not exist.
	GetAlias(ctx context.Context, aliasName string) (string, error)
	// CreateAlias atomically points the alias at the index, moving it from any
	// previous target.
	CreateAlias(ctx context.Context, aliasName, indexName string) error
}

// AliasMove classifies what it takes to bring a resource's read alias in line
// with the config's ReadVersion — the single owner of the alias target.
type AliasMove int

const (
	// AliasInSync: the alias already points at the ReadVersion index.
	AliasInSync AliasMove = iota
	// AliasCreate: the alias does not exist yet.
	AliasCreate
	// AliasForward: the alias points at an older version (a cutover).
	AliasForward
	// AliasBackward: the alias points at a newer version (a rollback).
	AliasBackward
	// AliasForeign: the alias points at an index this naming scheme does not
	// own (hand-built). Automated writers must not destroy it.
	AliasForeign
)

// String returns the move as a short word, stable for scripts and logs.
func (m AliasMove) String() string {
	switch m {
	case AliasInSync:
		return "in-sync"
	case AliasCreate:
		return "create"
	case AliasForward:
		return "forward"
	case AliasBackward:
		return "backward"
	case AliasForeign:
		return "foreign"
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}

// MarshalText makes AliasMove serialize as its String form in JSON reports.
func (m AliasMove) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

// PlanAliasMove classifies the move that would bring resourceType's read alias
// from its current target (empty = alias missing) to the readVersion index.
func PlanAliasMove(resourceType, currentTarget string, readVersion int) AliasMove {
	if currentTarget == "" {
		return AliasCreate
	}
	suffix, ok := strings.CutPrefix(currentTarget, resourceType+"_search_v")
	if !ok {
		return AliasForeign
	}
	current, err := strconv.Atoi(suffix)
	if err != nil || current <= 0 {
		return AliasForeign
	}
	switch {
	case current == readVersion:
		return AliasInSync
	case current < readVersion:
		return AliasForward
	default:
		return AliasBackward
	}
}

// ConvergeReadAliases points every resource's read alias at its config
// ReadVersion index. The config is the single owner of the alias target
// (see ADR 0009): a cutover or rollback is a readVersion change, and this is
// what applies it. Every resource is attempted; failures are aggregated. A
// missing target index fails the resource — the indices must be bootstrapped
// with gen-mapping before the indexer starts. An alias pointing at a
// hand-built index is skipped with a warning, never destroyed.
func ConvergeReadAliases(ctx context.Context, es AliasBackend, resources resource.Configs) error {
	var errs []error
	for _, cfg := range resources {
		aliasName := AliasName(cfg.Resource)
		desired := IndexName(cfg.Resource, cfg.ReadVersion)

		current, err := es.GetAlias(ctx, aliasName)
		if err != nil {
			errs = append(errs, fmt.Errorf("resource %q: get alias %s: %w", cfg.Resource, aliasName, err))
			continue
		}

		move := PlanAliasMove(cfg.Resource, current, cfg.ReadVersion)
		switch move {
		case AliasInSync:
			continue
		case AliasForeign:
			slog.Warn("read alias points at an unmanaged index, leaving it alone",
				slog.String("alias", aliasName), slog.String("target", current), slog.String("want", desired))
			continue
		case AliasBackward:
			slog.Warn("converging read alias backwards (readVersion rollback)",
				slog.String("alias", aliasName), slog.String("from", current), slog.String("to", desired))
		case AliasCreate, AliasForward:
			slog.Info("converging read alias",
				slog.String("alias", aliasName), slog.String("from", current), slog.String("to", desired))
		}

		if err := es.CreateAlias(ctx, aliasName, desired); err != nil {
			errs = append(errs, fmt.Errorf("resource %q: point alias %s at %s: %w", cfg.Resource, aliasName, desired, err))
		}
	}
	return errors.Join(errs...)
}
