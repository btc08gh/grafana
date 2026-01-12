package resource

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/dskit/ring"
	ringclient "github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	userutils "github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/services/featuremgmt"
	"github.com/grafana/grafana/pkg/services/grpcserver"
	"github.com/grafana/grafana/pkg/setting"
	"github.com/grafana/grafana/pkg/storage/unified/resourcepb"
)

func ProvideSearchDistributorServer(cfg *setting.Cfg, features featuremgmt.FeatureToggles, registerer prometheus.Registerer, tracer trace.Tracer, ring *ring.Ring, ringClientPool *ringclient.Pool) (grpcserver.Provider, error) {
	var err error
	grpcHandler, err := grpcserver.ProvideService(cfg, features, nil, tracer, registerer)
	if err != nil {
		return nil, err
	}

	distributorServer := &distributorServer{
		log:                    log.New("index-server-distributor"),
		ring:                   ring,
		clientPool:             ringClientPool,
		tracing:                tracer,
		subIndexesPerNamespace: cfg.SubIndexesPerNamespace,
	}

	healthService, err := ProvideHealthService(distributorServer)
	if err != nil {
		return nil, err
	}

	grpcServer := grpcHandler.GetServer()

	resourcepb.RegisterResourceIndexServer(grpcServer, distributorServer)
	resourcepb.RegisterManagedObjectIndexServer(grpcServer, distributorServer)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthService)
	_, err = grpcserver.ProvideReflectionService(cfg, grpcHandler)
	if err != nil {
		return nil, err
	}

	return grpcHandler, nil
}

type RingClient struct {
	Client ResourceClient
	grpc_health_v1.HealthClient
	Conn *grpc.ClientConn
}

func (c *RingClient) Close() error {
	return c.Conn.Close()
}

func (c *RingClient) String() string {
	return c.RemoteAddress()
}

func (c *RingClient) RemoteAddress() string {
	return c.Conn.Target()
}

const RingKey = "search-server-ring"
const RingName = "search_server_ring"
const RingHeartbeatTimeout = time.Minute
const RingNumTokens = 128

type distributorServer struct {
	clientPool             *ringclient.Pool
	ring                   *ring.Ring
	log                    log.Logger
	tracing                trace.Tracer
	subIndexesPerNamespace int // Number of sub-indexes per namespace (0 = disabled)
}

var (
	// operation used by the distributor to select only ACTIVE instances to handle search-related requests
	searchRingRead = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, func(s ring.InstanceState) bool {
		return s != ring.ACTIVE
	})
)

func (ds *distributorServer) Search(ctx context.Context, r *resourcepb.ResourceSearchRequest) (*resourcepb.ResourceSearchResponse, error) {
	ctx, span := ds.tracing.Start(ctx, "distributor.Search")
	defer span.End()

	// If sub-index sharding is not enabled, use the existing single-node routing
	if ds.subIndexesPerNamespace <= 0 {
		ctx, client, err := ds.getClientToDistributeRequest(ctx, r.Options.Key.Namespace, "Search")
		if err != nil {
			return nil, err
		}
		return client.Search(ctx, r)
	}

	// Scatter-gather search across all sub-indexes
	nsr := NamespacedResource{
		Namespace: r.Options.Key.Namespace,
		Group:     r.Options.Key.Group,
		Resource:  r.Options.Key.Resource,
	}

	span.SetAttributes(
		attribute.String("namespace", nsr.Namespace),
		attribute.String("group", nsr.Group),
		attribute.String("resource", nsr.Resource),
		attribute.Int("sub_indexes", ds.subIndexesPerNamespace),
	)

	subIndexes := ds.getSubIndexesForNamespace(nsr)
	results := ds.parallelSearchWithFailover(ctx, subIndexes, r)
	return ds.mergeResults(ctx, results, r)
}

func (ds *distributorServer) GetStats(ctx context.Context, r *resourcepb.ResourceStatsRequest) (*resourcepb.ResourceStatsResponse, error) {
	ctx, span := ds.tracing.Start(ctx, "distributor.GetStats")
	defer span.End()
	ctx, client, err := ds.getClientToDistributeRequest(ctx, r.Namespace, "GetStats")
	if err != nil {
		return nil, err
	}

	return client.GetStats(ctx, r)
}

func (ds *distributorServer) RebuildIndexes(ctx context.Context, r *resourcepb.RebuildIndexesRequest) (*resourcepb.RebuildIndexesResponse, error) {
	ctx, span := ds.tracing.Start(ctx, "distributor.RebuildIndexes")
	defer span.End()

	// validate input
	for _, key := range r.Keys {
		if r.Namespace != key.Namespace {
			return &resourcepb.RebuildIndexesResponse{
				Error: NewBadRequestError("key namespace does not match request namespace"),
			}, nil
		}
	}

	// distribute the request to all search pods to minimize risk of stale index
	// it will not rebuild on those which don't have the index open
	rs, err := ds.ring.GetAllHealthy(searchRingRead)
	if err != nil {
		return nil, fmt.Errorf("failed to get all healthy instances from the ring")
	}

	err = grpc.SetHeader(ctx, metadata.Pairs("proxied-instance-id", "all"))
	if err != nil {
		ds.log.Debug("error setting grpc header", "err", err)
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = make(metadata.MD)
	}
	rCtx := userutils.InjectOrgID(metadata.NewOutgoingContext(ctx, md), r.Namespace)

	var wg sync.WaitGroup
	var totalRebuildCount atomic.Int64
	detailsCh := make(chan string, len(rs.Instances))
	errorCh := make(chan error, len(rs.Instances))

	for _, inst := range rs.Instances {
		wg.Add(1)
		go func() {
			defer wg.Done()

			client, err := ds.clientPool.GetClientForInstance(inst)
			if err != nil {
				errorCh <- fmt.Errorf("instance %s: failed to get client, %w", inst.Id, err)
				return
			}

			rsp, err := client.(*RingClient).Client.RebuildIndexes(rCtx, r)
			if err != nil {
				errorCh <- fmt.Errorf("instance %s: failed to distribute rebuild index request, %w", inst.Id, err)
				return
			}

			if rsp.Error != nil {
				errorCh <- fmt.Errorf("instance %s: rebuild index request returned the error %s", inst.Id, rsp.Error.Message)
				return
			}

			if rsp.Details != "" {
				detailsCh <- fmt.Sprintf("{instance: %s, details: %s}", inst.Id, rsp.Details)
			}

			totalRebuildCount.Add(rsp.RebuildCount)
		}()
	}

	wg.Wait()
	close(errorCh)
	close(detailsCh)

	errs := make([]error, 0, len(errorCh))
	for err := range errorCh {
		ds.log.Error("rebuild indexes call failed with %w", err)
		errs = append(errs, err)
	}

	var details string
	for d := range detailsCh {
		if len(details) > 0 {
			details += ", "
		}
		details += d
	}

	response := &resourcepb.RebuildIndexesResponse{
		RebuildCount: totalRebuildCount.Load(),
		Details:      details,
	}
	if len(errs) > 0 {
		response.Error = AsErrorResult(errors.Join(errs...))
	}
	return response, nil
}

func (ds *distributorServer) CountManagedObjects(ctx context.Context, r *resourcepb.CountManagedObjectsRequest) (*resourcepb.CountManagedObjectsResponse, error) {
	ctx, span := ds.tracing.Start(ctx, "distributor.CountManagedObjects")
	defer span.End()
	ctx, client, err := ds.getClientToDistributeRequest(ctx, r.Namespace, "CountManagedObjects")
	if err != nil {
		return nil, err
	}

	return client.CountManagedObjects(ctx, r)
}

func (ds *distributorServer) ListManagedObjects(ctx context.Context, r *resourcepb.ListManagedObjectsRequest) (*resourcepb.ListManagedObjectsResponse, error) {
	ctx, span := ds.tracing.Start(ctx, "distributor.ListManagedObjects")
	defer span.End()
	ctx, client, err := ds.getClientToDistributeRequest(ctx, r.Namespace, "ListManagedObjects")
	if err != nil {
		return nil, err
	}

	return client.ListManagedObjects(ctx, r)
}

func (ds *distributorServer) getClientToDistributeRequest(ctx context.Context, namespace string, methodName string) (context.Context, ResourceClient, error) {
	ringHasher := fnv.New32a()
	_, err := ringHasher.Write([]byte(namespace))
	if err != nil {
		ds.log.Debug("error hashing namespace", "err", err, "namespace", namespace)
		return ctx, nil, err
	}

	rs, err := ds.ring.GetWithOptions(ringHasher.Sum32(), searchRingRead)
	if err != nil {
		ds.log.Debug("error getting replication set from ring", "err", err, "namespace", namespace)
		return ctx, nil, err
	}

	// Randomly select an instance for primitive load balancing
	inst := rs.Instances[rand.Intn(len(rs.Instances))]
	client, err := ds.clientPool.GetClientForInstance(inst)
	if err != nil {
		ds.log.Debug("error getting instance client from pool", "err", err, "namespace", namespace, "searchApiInstanceId", inst.Id)
		return ctx, nil, err
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = make(metadata.MD)
	}

	err = grpc.SetHeader(ctx, metadata.Pairs("proxied-instance-id", inst.Id))
	if err != nil {
		ds.log.Debug("error setting grpc header", "err", err)
	}

	return userutils.InjectOrgID(metadata.NewOutgoingContext(ctx, md), namespace), client.(*RingClient).Client, nil
}

func (ds *distributorServer) IsHealthy(ctx context.Context, r *resourcepb.HealthCheckRequest) (*resourcepb.HealthCheckResponse, error) {
	if ds.ring.State() == services.Running {
		return &resourcepb.HealthCheckResponse{Status: resourcepb.HealthCheckResponse_SERVING}, nil
	}

	return &resourcepb.HealthCheckResponse{Status: resourcepb.HealthCheckResponse_NOT_SERVING}, nil
}

// --- Scatter-Gather Query Implementation with Replica Failover ---

// subIndexSearchResult holds the result from searching a single sub-index.
type subIndexSearchResult struct {
	subIndexID     int
	response       *resourcepb.ResourceSearchResponse
	err            error
	partialFailure bool // true if all replicas for this sub-index failed
}

// getSubIndexesForNamespace returns all sub-index keys for a NamespacedResource.
// This is used for scatter-gather queries that need to query all sub-indexes.
func (ds *distributorServer) getSubIndexesForNamespace(nsr NamespacedResource) []SubIndexKey {
	count := ds.subIndexesPerNamespace
	if count <= 0 {
		count = 1
	}
	keys := make([]SubIndexKey, count)
	for i := 0; i < count; i++ {
		keys[i] = SubIndexKey{
			NamespacedResource: nsr,
			SubIndexID:         i,
		}
	}
	return keys
}

// getReplicasForSubIndex returns the ordered list of replicas (instances) that own
// the given sub-index. Replicas are ordered by preference from the ring.
// Uses consistent hashing including the sub-index ID to determine ownership.
func (ds *distributorServer) getReplicasForSubIndex(subIndex SubIndexKey) ([]ring.InstanceDesc, error) {
	ringHasher := fnv.New32a()
	// Include sub-index ID in hash to distribute sub-indexes across nodes
	_, err := ringHasher.Write([]byte(fmt.Sprintf("%s/%d", subIndex.Namespace, subIndex.SubIndexID)))
	if err != nil {
		return nil, fmt.Errorf("error hashing sub-index key: %w", err)
	}
	rs, err := ds.ring.GetWithOptions(ringHasher.Sum32(), searchRingRead)
	if err != nil {
		return nil, fmt.Errorf("error getting replication set from ring for sub-index %s: %w", subIndex.String(), err)
	}

	return rs.Instances, nil
}

// parallelSearchWithFailover executes search queries across all sub-indexes in parallel.
// For each sub-index, it tries replicas in order until one succeeds.
// Returns results from all sub-indexes (some may be marked as partial failures).
func (ds *distributorServer) parallelSearchWithFailover(ctx context.Context, subIndexes []SubIndexKey, r *resourcepb.ResourceSearchRequest) []*subIndexSearchResult {
	ctx, span := ds.tracing.Start(ctx, "distributor.parallelSearchWithFailover")
	defer span.End()

	results := make([]*subIndexSearchResult, len(subIndexes))
	var wg sync.WaitGroup

	for i, subIdx := range subIndexes {
		wg.Add(1)
		go func(idx int, key SubIndexKey) {
			defer wg.Done()
			results[idx] = ds.searchSubIndexWithFailover(ctx, key, r)
		}(i, subIdx)
	}

	wg.Wait()

	// Count partial failures for logging
	partialFailures := 0
	for _, result := range results {
		if result.partialFailure {
			partialFailures++
		}
	}
	span.SetAttributes(attribute.Int("partial_failures", partialFailures))

	return results
}

// searchSubIndexWithFailover searches a single sub-index, trying each replica in order
// until one succeeds. If all replicas fail, marks the result as a partial failure.
func (ds *distributorServer) searchSubIndexWithFailover(ctx context.Context, subIndex SubIndexKey, r *resourcepb.ResourceSearchRequest) *subIndexSearchResult {
	result := &subIndexSearchResult{
		subIndexID: subIndex.SubIndexID,
	}

	// Get ordered list of replicas for this sub-index
	replicas, err := ds.getReplicasForSubIndex(subIndex)
	if err != nil {
		ds.log.Warn("failed to get replicas for sub-index", "subIndex", subIndex.String(), "error", err)
		result.err = err
		result.partialFailure = true
		return result
	}

	if len(replicas) == 0 {
		ds.log.Warn("no replicas available for sub-index", "subIndex", subIndex.String())
		result.err = fmt.Errorf("no replicas available for sub-index %s", subIndex.String())
		result.partialFailure = true
		return result
	}

	// Prepare context with metadata
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = make(metadata.MD)
	}
	rCtx := userutils.InjectOrgID(metadata.NewOutgoingContext(ctx, md), subIndex.Namespace)

	// Try each replica in order until success
	// Per-replica timeout ensures SLO compliance (≤500ms for distributed search)
	// With replication factor 2: worst case is 2 attempts × 200ms = 400ms, leaving room for merge
	const replicaTimeout = 200 * time.Millisecond
	var lastErr error
	for replicaIdx, replica := range replicas {
		client, err := ds.clientPool.GetClientForInstance(replica)
		if err != nil {
			ds.log.Debug("failed to get client for replica",
				"subIndex", subIndex.String(),
				"replica", replica.Id,
				"replicaIdx", replicaIdx,
				"error", err)
			lastErr = err
			continue
		}

		// Apply per-replica timeout to ensure failover happens quickly
		replicaCtx, cancel := context.WithTimeout(rCtx, replicaTimeout)
		resp, err := client.(*RingClient).Client.Search(replicaCtx, r)
		cancel() // Always cancel to release resources

		if err == nil && resp.Error == nil {
			// Success
			result.response = resp
			return result
		}

		// Log failover event
		if err != nil {
			lastErr = err
			ds.log.Warn("search failed on replica, failing over to next",
				"subIndex", subIndex.String(),
				"replica", replica.Id,
				"replicaIdx", replicaIdx,
				"remainingReplicas", len(replicas)-replicaIdx-1,
				"error", err)
		} else if resp.Error != nil {
			lastErr = fmt.Errorf("search error: %s", resp.Error.Message)
			ds.log.Warn("search returned error on replica, failing over to next",
				"subIndex", subIndex.String(),
				"replica", replica.Id,
				"replicaIdx", replicaIdx,
				"remainingReplicas", len(replicas)-replicaIdx-1,
				"errorMessage", resp.Error.Message)
		}
	}

	// All replicas failed - mark as partial failure
	ds.log.Error("all replicas failed for sub-index",
		"subIndex", subIndex.String(),
		"totalReplicas", len(replicas),
		"lastError", lastErr)
	result.err = lastErr
	result.partialFailure = true
	return result
}

// mergeResults combines results from all sub-indexes into a single response.
// It handles:
// - Result deduplication by resource key
// - Sort merging (merge-sort for sorted results)
// - Pagination across shards
// - Tracking and reporting partial failures
func (ds *distributorServer) mergeResults(ctx context.Context, results []*subIndexSearchResult, req *resourcepb.ResourceSearchRequest) (*resourcepb.ResourceSearchResponse, error) {
	ctx, span := ds.tracing.Start(ctx, "distributor.mergeResults")
	defer span.End()

	// Collect all rows and track partial failures
	var allRows []*resourcepb.ResourceTableRow
	var columns []*resourcepb.ResourceTableColumnDefinition
	var totalHits int64
	var totalQueryCost float64
	var maxScore float64
	partialFailures := 0
	failedSubIndexes := []int{}
	facets := make(map[string]*resourcepb.ResourceSearchResponse_Facet)

	for _, result := range results {
		if result.partialFailure {
			partialFailures++
			failedSubIndexes = append(failedSubIndexes, result.subIndexID)
			continue
		}

		if result.response == nil {
			continue
		}

		resp := result.response
		totalHits += resp.TotalHits
		totalQueryCost += resp.QueryCost
		if resp.MaxScore > maxScore {
			maxScore = resp.MaxScore
		}

		// Use columns from first valid response
		if columns == nil && resp.Results != nil {
			columns = resp.Results.Columns
		}

		// Collect rows
		if resp.Results != nil && len(resp.Results.Rows) > 0 {
			allRows = append(allRows, resp.Results.Rows...)
		}

		// Merge facets
		for k, v := range resp.Facet {
			if existing, ok := facets[k]; ok {
				mergeFacets(existing, v)
			} else {
				facets[k] = v
			}
		}
	}

	span.SetAttributes(
		attribute.Int("total_rows_before_dedup", len(allRows)),
		attribute.Int("partial_failures", partialFailures),
	)

	// Deduplicate rows by resource key
	allRows = deduplicateRows(allRows)

	span.SetAttributes(attribute.Int("total_rows_after_dedup", len(allRows)))

	// Sort rows if sort criteria provided
	if len(req.SortBy) > 0 && columns != nil {
		sortRows(allRows, columns, req.SortBy)
	}

	// Apply pagination
	offset := int(req.Offset)
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100 // default limit
	}

	var paginatedRows []*resourcepb.ResourceTableRow
	if offset < len(allRows) {
		end := offset + limit
		if end > len(allRows) {
			end = len(allRows)
		}
		paginatedRows = allRows[offset:end]
	}

	// Build response
	response := &resourcepb.ResourceSearchResponse{
		TotalHits: totalHits,
		QueryCost: totalQueryCost,
		MaxScore:  maxScore,
		Results: &resourcepb.ResourceTable{
			Columns: columns,
			Rows:    paginatedRows,
		},
		Facet: facets,
	}

	// Report partial failures if any
	if partialFailures > 0 {
		response.Error = &resourcepb.ErrorResult{
			Code:    206, // Partial Content
			Message: fmt.Sprintf("partial results: %d of %d sub-indexes failed (sub-indexes: %v)", partialFailures, len(results), failedSubIndexes),
		}
		ds.log.Warn("search returned partial results",
			"namespace", req.Options.Key.Namespace,
			"failedSubIndexes", partialFailures,
			"totalSubIndexes", len(results))
	}

	return response, nil
}

// deduplicateRows removes duplicate rows based on their resource key.
// If duplicates exist, keeps the first occurrence.
func deduplicateRows(rows []*resourcepb.ResourceTableRow) []*resourcepb.ResourceTableRow {
	if len(rows) == 0 {
		return rows
	}

	seen := make(map[string]bool, len(rows))
	result := make([]*resourcepb.ResourceTableRow, 0, len(rows))

	for _, row := range rows {
		if row.Key == nil {
			continue
		}
		key := SearchID(row.Key)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}

	return result
}

// sortRows sorts rows based on the sort criteria.
// Uses stable sort to maintain relative ordering of equal elements.
func sortRows(rows []*resourcepb.ResourceTableRow, columns []*resourcepb.ResourceTableColumnDefinition, sortBy []*resourcepb.ResourceSearchRequest_Sort) {
	if len(rows) == 0 || len(sortBy) == 0 {
		return
	}

	// Build column index map
	columnIndex := make(map[string]int)
	for i, col := range columns {
		columnIndex[col.Name] = i
	}

	sort.SliceStable(rows, func(i, j int) bool {
		for _, s := range sortBy {
			colIdx, ok := columnIndex[s.Field]
			if !ok {
				continue
			}

			// Get cell values
			var valI, valJ []byte
			if colIdx < len(rows[i].Cells) {
				valI = rows[i].Cells[colIdx]
			}
			if colIdx < len(rows[j].Cells) {
				valJ = rows[j].Cells[colIdx]
			}

			// Compare byte slices
			cmpResult := slices.Compare(valI, valJ)
			if cmpResult == 0 {
				continue // Values are equal, check next sort field
			}

			// Apply descending order if needed
			if s.Desc {
				return cmpResult > 0
			}
			return cmpResult < 0
		}
		return false // All sort fields are equal
	})
}

// mergeFacets merges facet data from source into target.
func mergeFacets(target, source *resourcepb.ResourceSearchResponse_Facet) {
	if source == nil {
		return
	}

	target.Total += source.Total
	target.Missing += source.Missing

	// Merge term facets
	termMap := make(map[string]int64)
	for _, t := range target.Terms {
		termMap[t.Term] = t.Count
	}
	for _, t := range source.Terms {
		termMap[t.Term] += t.Count
	}

	// Rebuild term slice sorted by count (descending)
	target.Terms = make([]*resourcepb.ResourceSearchResponse_TermFacet, 0, len(termMap))
	for term, count := range termMap {
		target.Terms = append(target.Terms, &resourcepb.ResourceSearchResponse_TermFacet{
			Term:  term,
			Count: count,
		})
	}
	slices.SortFunc(target.Terms, func(a, b *resourcepb.ResourceSearchResponse_TermFacet) int {
		return cmp.Compare(b.Count, a.Count) // Descending order
	})
}
