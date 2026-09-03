package core

import (
	"fmt"

	"github.com/theleeeo/laika/core/resource"
)

// GetCapabilities returns the search capabilities for all configured resources.
// Each resource advertises every active schema version (ascending) plus the
// read version's fields at the top level for clients that don't care about
// versions.
func (idx *Indexer) GetCapabilities() CapabilitiesResponse {
	resp := CapabilitiesResponse{}

	for _, rc := range idx.resources {
		cap := ResourceCapability{Resource: rc.Resource, ReadVersion: rc.ReadVersion}

		for _, v := range rc.SortedVersions() {
			vc := rc.GetVersion(v)
			cap.Versions = append(cap.Versions, VersionCapability{
				Version: v,
				Fields:  versionFieldCapabilities(vc),
			})
			if v == rc.ReadVersion {
				cap.Fields = cap.Versions[len(cap.Versions)-1].Fields
			}
		}
		// A ReadVersion with no matching VersionConfig (validation rejects
		// this; guard against library misuse) leaves Fields empty.

		resp.Resources = append(resp.Resources, cap)
	}

	return resp
}

// versionFieldCapabilities flattens one schema version's fields, relations and
// nested blocks into the advertised field list.
func versionFieldCapabilities(vc *resource.VersionConfig) []FieldCapability {
	var fields []FieldCapability

	for _, f := range vc.Fields {
		fields = append(fields, fieldCapability("fields."+f.Name, f))
	}

	for _, rel := range vc.Relations {
		for _, f := range rel.Fields {
			fc := fieldCapability(fmt.Sprintf("%s.%s", rel.Resource, f.Name), f)
			if rel.IsReference() {
				// Resolved via a child search at query time: never part of the
				// parent's full-text surface or sort, and negation ops are not
				// offered because the join gives them "some joined child
				// differs" semantics instead of the document-level "no child
				// matches" (see withoutNegations).
				fc.Searchable = false
				fc.Sortable = false
				fc.FilterOps = withoutNegations(fc.FilterOps)
			}
			fields = append(fields, fc)
		}
	}

	for _, b := range vc.NestedBlocks {
		for _, f := range b.Fields {
			fields = append(fields, fieldCapability(fmt.Sprintf("%s.%s", b.Name, f.Name), f))
		}
	}

	return fields
}

func fieldCapability(path string, f resource.FieldConfig) FieldCapability {
	esType := f.ESType()
	return FieldCapability{
		Field:      path,
		Type:       esType,
		Searchable: f.Query.IsSearchable(),
		Sortable:   esType != "text",
		FilterOps:  OpsForField(f),
	}
}
