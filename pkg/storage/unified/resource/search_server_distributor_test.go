package resource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/storage/unified/resourcepb"
)

func TestGetSubIndexesForNamespace(t *testing.T) {
	tests := []struct {
		name          string
		subIndexCount int
		expectedCount int
		expectedIDs   []int
	}{
		{
			name:          "zero sub-indexes defaults to 1",
			subIndexCount: 0,
			expectedCount: 1,
			expectedIDs:   []int{0},
		},
		{
			name:          "negative sub-indexes defaults to 1",
			subIndexCount: -1,
			expectedCount: 1,
			expectedIDs:   []int{0},
		},
		{
			name:          "4 sub-indexes",
			subIndexCount: 4,
			expectedCount: 4,
			expectedIDs:   []int{0, 1, 2, 3},
		},
		{
			name:          "64 sub-indexes",
			subIndexCount: 64,
			expectedCount: 64,
			expectedIDs:   nil, // Don't check all 64
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := &distributorServer{
				subIndexesPerNamespace: tt.subIndexCount,
			}

			nsr := NamespacedResource{
				Namespace: "org-1",
				Group:     "dashboard.grafana.app",
				Resource:  "dashboards",
			}

			result := ds.getSubIndexesForNamespace(nsr)
			assert.Len(t, result, tt.expectedCount)

			// Check that all keys have the correct NSR
			for i, key := range result {
				assert.Equal(t, nsr, key.NamespacedResource)
				assert.Equal(t, i, key.SubIndexID)
			}

			// Check specific IDs if provided
			if tt.expectedIDs != nil {
				for i, expectedID := range tt.expectedIDs {
					assert.Equal(t, expectedID, result[i].SubIndexID)
				}
			}
		})
	}
}

func TestDeduplicateRows(t *testing.T) {
	tests := []struct {
		name     string
		input    []*resourcepb.ResourceTableRow
		expected int // expected number of rows after dedup
	}{
		{
			name:     "empty input",
			input:    []*resourcepb.ResourceTableRow{},
			expected: 0,
		},
		{
			name: "no duplicates",
			input: []*resourcepb.ResourceTableRow{
				{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "a"}},
				{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "b"}},
				{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "c"}},
			},
			expected: 3,
		},
		{
			name: "with duplicates",
			input: []*resourcepb.ResourceTableRow{
				{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "a"}},
				{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "b"}},
				{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "a"}}, // duplicate
				{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "c"}},
				{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "b"}}, // duplicate
			},
			expected: 3,
		},
		{
			name: "rows with nil keys are skipped",
			input: []*resourcepb.ResourceTableRow{
				{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "a"}},
				{Key: nil},
				{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "b"}},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := deduplicateRows(tt.input)
			assert.Len(t, result, tt.expected)
		})
	}
}

func TestSortRows(t *testing.T) {
	columns := []*resourcepb.ResourceTableColumnDefinition{
		{Name: "title"},
		{Name: "created"},
	}

	tests := []struct {
		name          string
		rows          []*resourcepb.ResourceTableRow
		sortBy        []*resourcepb.ResourceSearchRequest_Sort
		expectedOrder []string // expected order of first cell values
	}{
		{
			name: "sort ascending by title",
			rows: []*resourcepb.ResourceTableRow{
				{Key: &resourcepb.ResourceKey{Name: "c"}, Cells: [][]byte{[]byte("charlie"), nil}},
				{Key: &resourcepb.ResourceKey{Name: "a"}, Cells: [][]byte{[]byte("alpha"), nil}},
				{Key: &resourcepb.ResourceKey{Name: "b"}, Cells: [][]byte{[]byte("bravo"), nil}},
			},
			sortBy: []*resourcepb.ResourceSearchRequest_Sort{
				{Field: "title", Desc: false},
			},
			expectedOrder: []string{"alpha", "bravo", "charlie"},
		},
		{
			name: "sort descending by title",
			rows: []*resourcepb.ResourceTableRow{
				{Key: &resourcepb.ResourceKey{Name: "a"}, Cells: [][]byte{[]byte("alpha"), nil}},
				{Key: &resourcepb.ResourceKey{Name: "b"}, Cells: [][]byte{[]byte("bravo"), nil}},
				{Key: &resourcepb.ResourceKey{Name: "c"}, Cells: [][]byte{[]byte("charlie"), nil}},
			},
			sortBy: []*resourcepb.ResourceSearchRequest_Sort{
				{Field: "title", Desc: true},
			},
			expectedOrder: []string{"charlie", "bravo", "alpha"},
		},
		{
			name:          "empty rows",
			rows:          []*resourcepb.ResourceTableRow{},
			sortBy:        []*resourcepb.ResourceSearchRequest_Sort{{Field: "title"}},
			expectedOrder: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sortRows(tt.rows, columns, tt.sortBy)

			for i, expected := range tt.expectedOrder {
				if i < len(tt.rows) {
					assert.Equal(t, expected, string(tt.rows[i].Cells[0]))
				}
			}
		})
	}
}

func TestMergeFacets(t *testing.T) {
	t.Run("merge term facets", func(t *testing.T) {
		target := &resourcepb.ResourceSearchResponse_Facet{
			Field:   "tags",
			Total:   100,
			Missing: 5,
			Terms: []*resourcepb.ResourceSearchResponse_TermFacet{
				{Term: "production", Count: 50},
				{Term: "staging", Count: 30},
			},
		}

		source := &resourcepb.ResourceSearchResponse_Facet{
			Field:   "tags",
			Total:   80,
			Missing: 3,
			Terms: []*resourcepb.ResourceSearchResponse_TermFacet{
				{Term: "production", Count: 40},
				{Term: "development", Count: 25},
			},
		}

		mergeFacets(target, source)

		assert.Equal(t, int64(180), target.Total)
		assert.Equal(t, int64(8), target.Missing)
		assert.Len(t, target.Terms, 3)

		// Terms should be sorted by count descending
		termCounts := make(map[string]int64)
		for _, term := range target.Terms {
			termCounts[term.Term] = term.Count
		}
		assert.Equal(t, int64(90), termCounts["production"])
		assert.Equal(t, int64(30), termCounts["staging"])
		assert.Equal(t, int64(25), termCounts["development"])
	})

	t.Run("merge nil source", func(t *testing.T) {
		target := &resourcepb.ResourceSearchResponse_Facet{
			Total: 100,
		}
		mergeFacets(target, nil)
		assert.Equal(t, int64(100), target.Total)
	})
}

func TestSubIndexSearchResult(t *testing.T) {
	t.Run("successful result", func(t *testing.T) {
		result := &subIndexSearchResult{
			subIndexID: 5,
			response: &resourcepb.ResourceSearchResponse{
				TotalHits: 100,
			},
			partialFailure: false,
		}
		assert.False(t, result.partialFailure)
		assert.NotNil(t, result.response)
	})

	t.Run("partial failure result", func(t *testing.T) {
		result := &subIndexSearchResult{
			subIndexID:     3,
			err:            assert.AnError,
			partialFailure: true,
		}
		assert.True(t, result.partialFailure)
		assert.Nil(t, result.response)
		assert.Error(t, result.err)
	})
}

func TestSubIndexKey(t *testing.T) {
	key := SubIndexKey{
		NamespacedResource: NamespacedResource{
			Namespace: "org-1",
			Group:     "dashboard.grafana.app",
			Resource:  "dashboards",
		},
		SubIndexID: 42,
	}

	t.Run("String representation", func(t *testing.T) {
		expected := "org-1/dashboard.grafana.app/dashboards/shard-42"
		assert.Equal(t, expected, key.String())
	})

	t.Run("ToNamespacedResource", func(t *testing.T) {
		nsr := key.ToNamespacedResource()
		assert.Equal(t, "org-1", nsr.Namespace)
		assert.Equal(t, "dashboard.grafana.app", nsr.Group)
		assert.Equal(t, "dashboards", nsr.Resource)
	})
}

func newTestDistributorServer() *distributorServer {
	return &distributorServer{
		tracing: noop.NewTracerProvider().Tracer("test"),
		log:     log.New("test-distributor"),
	}
}

func TestMergeResultsPagination(t *testing.T) {
	// Create mock results from 3 sub-indexes
	results := []*subIndexSearchResult{
		{
			subIndexID: 0,
			response: &resourcepb.ResourceSearchResponse{
				TotalHits: 10,
				Results: &resourcepb.ResourceTable{
					Columns: []*resourcepb.ResourceTableColumnDefinition{{Name: "title"}},
					Rows: []*resourcepb.ResourceTableRow{
						{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "a1"}, Cells: [][]byte{[]byte("a1")}},
						{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "a2"}, Cells: [][]byte{[]byte("a2")}},
					},
				},
			},
		},
		{
			subIndexID: 1,
			response: &resourcepb.ResourceSearchResponse{
				TotalHits: 10,
				Results: &resourcepb.ResourceTable{
					Columns: []*resourcepb.ResourceTableColumnDefinition{{Name: "title"}},
					Rows: []*resourcepb.ResourceTableRow{
						{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "b1"}, Cells: [][]byte{[]byte("b1")}},
						{Key: &resourcepb.ResourceKey{Namespace: "ns", Group: "g", Resource: "r", Name: "b2"}, Cells: [][]byte{[]byte("b2")}},
					},
				},
			},
		},
	}

	t.Run("pagination with limit", func(t *testing.T) {
		ds := newTestDistributorServer()
		req := &resourcepb.ResourceSearchRequest{
			Limit:  2,
			Offset: 0,
			Options: &resourcepb.ListOptions{
				Key: &resourcepb.ResourceKey{Namespace: "ns"},
			},
		}

		resp, err := ds.mergeResults(context.Background(), results, req)
		require.NoError(t, err)
		assert.Equal(t, int64(20), resp.TotalHits) // 10 + 10
		assert.Len(t, resp.Results.Rows, 2)
	})

	t.Run("pagination with offset", func(t *testing.T) {
		ds := newTestDistributorServer()
		req := &resourcepb.ResourceSearchRequest{
			Limit:  2,
			Offset: 2,
			Options: &resourcepb.ListOptions{
				Key: &resourcepb.ResourceKey{Namespace: "ns"},
			},
		}

		resp, err := ds.mergeResults(context.Background(), results, req)
		require.NoError(t, err)
		assert.Len(t, resp.Results.Rows, 2) // rows 3 and 4
	})
}

func TestMergeResultsPartialFailure(t *testing.T) {
	results := []*subIndexSearchResult{
		{
			subIndexID: 0,
			response: &resourcepb.ResourceSearchResponse{
				TotalHits: 10,
				Results: &resourcepb.ResourceTable{
					Columns: []*resourcepb.ResourceTableColumnDefinition{{Name: "title"}},
					Rows:    []*resourcepb.ResourceTableRow{},
				},
			},
		},
		{
			subIndexID:     1,
			partialFailure: true,
			err:            assert.AnError,
		},
		{
			subIndexID:     2,
			partialFailure: true,
			err:            assert.AnError,
		},
	}

	ds := newTestDistributorServer()
	req := &resourcepb.ResourceSearchRequest{
		Limit: 100,
		Options: &resourcepb.ListOptions{
			Key: &resourcepb.ResourceKey{Namespace: "ns"},
		},
	}

	resp, err := ds.mergeResults(context.Background(), results, req)
	require.NoError(t, err)

	// Should have partial failure error
	require.NotNil(t, resp.Error)
	assert.Equal(t, int32(206), resp.Error.Code) // HTTP 206 Partial Content
	assert.Contains(t, resp.Error.Message, "2 of 3 sub-indexes failed")
}
