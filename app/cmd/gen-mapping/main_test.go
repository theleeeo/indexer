package main

import (
	"testing"

	"github.com/theleeeo/laika/core"
)

func TestDecideAliasApply(t *testing.T) {
	cases := []struct {
		name      string
		move      core.AliasMove
		force     bool
		wantApply bool
		wantErr   bool
	}{
		{"in sync is a no-op", core.AliasInSync, false, false, false},
		{"missing alias is created", core.AliasCreate, false, true, false},
		{"forward cutover applies", core.AliasForward, false, true, false},
		{"backward move refused without force", core.AliasBackward, false, false, true},
		{"backward move applies with force", core.AliasBackward, true, true, false},
		{"foreign target refused without force", core.AliasForeign, false, false, true},
		{"foreign target applies with force", core.AliasForeign, true, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			apply, err := decideAliasApply(tc.move, tc.force)
			if apply != tc.wantApply {
				t.Fatalf("apply = %v, want %v", apply, tc.wantApply)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
