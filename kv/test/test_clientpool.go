package kvtest

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"cs426.yale.edu/lab4/kv"
	"cs426.yale.edu/lab4/kv/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type TestClientPool struct {
	mutex           sync.RWMutex
	getClientErrors map[string]error
	nodes           map[string]*TestClient
}

func (cp *TestClientPool) Setup(nodes map[string]*kv.KvServerImpl) {
	cp.nodes = make(map[string]*TestClient)
	for nodeName, server := range nodes {
		cp.nodes[nodeName] = &TestClient{
			server: server,
			err:    nil,
		}
	}
}

func (cp *TestClientPool) GetClient(nodeName string) (proto.KvClient, error) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	err, ok := cp.getClientErrors[nodeName]
	if ok {
		return nil, err
	}

	if cp.nodes == nil {
		return nil, errors.Errorf("node %s node setup yet: test cluster may be starting", nodeName)
	}
	return cp.nodes[nodeName], nil
}

func (cp *TestClientPool) OverrideGetClientError(nodeName string, err error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	if cp.getClientErrors == nil {
		cp.getClientErrors = make(map[string]error)
	}
	cp.getClientErrors[nodeName] = err
}
func (cp *TestClientPool) ClearGetClientErrors() {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	cp.getClientErrors = nil
}
func (cp *TestClientPool) OverrideRpcError(nodeName string, err error) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	cp.nodes[nodeName].OverrideError(err)
}
func (cp *TestClientPool) OverrideGetResponse(nodeName string, val string, wasFound bool) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	cp.nodes[nodeName].OverrideGetResponse(val, wasFound)
}
func (cp *TestClientPool) OverrideSetResponse(nodeName string) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	cp.nodes[nodeName].OverrideSetResponse()
}
func (cp *TestClientPool) OverrideDeleteResponse(nodeName string) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	cp.nodes[nodeName].OverrideDeleteResponse()
}

func (cp *TestClientPool) OverrideMultiSetResponse(nodeName string, failedKeys []string) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	cp.nodes[nodeName].OverrideMultiSetResponse(failedKeys)
}

func (cp *TestClientPool) OverrideGetShardContentsResponse(nodeName string, response *proto.GetShardContentsResponse) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	cp.nodes[nodeName].OverrideGetShardContentsResponse(response)
}
func (cp *TestClientPool) AddLatencyInjection(nodeName string, duration time.Duration) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	cp.nodes[nodeName].SetLatencyInjection(duration)
}
func (cp *TestClientPool) ClearRpcOverrides(nodeName string) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	cp.nodes[nodeName].ClearOverrides()
}
func (cp *TestClientPool) GetRequestsSent(nodeName string) int {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	client := cp.nodes[nodeName]
	return int(atomic.LoadUint64(&client.requestsSent))
}
func (cp *TestClientPool) ClearRequestsSent(nodeName string) {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()
	client := cp.nodes[nodeName]
	atomic.StoreUint64(&client.requestsSent, 0)
}

func (cp *TestClientPool) ClearServerImpls() {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()
	for nodeName := range cp.nodes {
		cp.nodes[nodeName].server = nil
	}
}

type TestClient struct {
	server *kv.KvServerImpl
	// requestsSent is managed atomically so we don't need write locks per request
	requestsSent uint64

	// mutex protects below variables which act as mock responses
	// for testing
	mutex                    sync.RWMutex
	err                      error
	getResponse              *proto.GetResponse
	setResponse              *proto.SetResponse
	deleteResponse           *proto.DeleteResponse
	getShardContentsResponse *proto.GetShardContentsResponse

	createListResponse      *proto.CreateListResponse
	createSetResponse       *proto.CreateSetResponse
	createSortedSetResponse *proto.CreateSortedSetResponse

	appendListResponse      *proto.AppendListResponse
	appendSetResponse       *proto.AppendSetResponse
	appendSortedSetResponse *proto.AppendSortedSetResponse

	popListResponse         *proto.PopListResponse
	removeListResponse      *proto.RemoveListResponse
	removeSetResponse       *proto.RemoveSetResponse
	removeSortedSetResponse *proto.RemoveSortedSetResponse

	checkListResponse      *proto.CheckListResponse
	checkSetResponse       *proto.CheckSetResponse
	checkSortedSetResponse *proto.CheckSortedSetResponse

	multiSetResponse *proto.MultiSetResponse
	cASResponse      *proto.CASResponse
	getRangeResponse *proto.GetRangeResponse

	latencyInjection *time.Duration
}

func (c *TestClient) Get(ctx context.Context, req *proto.GetRequest, opts ...grpc.CallOption) (*proto.GetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getResponse != nil {
		return c.getResponse, nil
	}
	return c.server.Get(ctx, req)
}
func (c *TestClient) Set(ctx context.Context, req *proto.SetRequest, opts ...grpc.CallOption) (*proto.SetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)

	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {

		time.Sleep(*c.latencyInjection)
	}
	if c.setResponse != nil {
		log.Printf("HI server %p", c.server)
		return c.setResponse, nil
	}

	return c.server.Set(ctx, req)
}
func (c *TestClient) Delete(ctx context.Context, req *proto.DeleteRequest, opts ...grpc.CallOption) (*proto.DeleteResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.deleteResponse != nil {
		return c.deleteResponse, nil
	}
	return c.server.Delete(ctx, req)
}
func (c *TestClient) GetShardContents(ctx context.Context, req *proto.GetShardContentsRequest, opts ...grpc.CallOption) (proto.Kv_GetShardContentsClient, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return nil, nil
	}
	// TODO: and fix this (BEN)
	// return c.server.GetShardContents(req,)
	return nil, nil
}

func (c *TestClient) ClearOverrides() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.err = nil
	c.getResponse = nil
	c.getShardContentsResponse = nil
	c.latencyInjection = nil
}

func (c *TestClient) OverrideError(err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.err = err
}

func (c *TestClient) OverrideGetResponse(val string, wasFound bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.getResponse = &proto.GetResponse{
		Value:    val,
		WasFound: wasFound,
	}
}
func (c *TestClient) OverrideSetResponse() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.setResponse = &proto.SetResponse{}
}
func (c *TestClient) OverrideDeleteResponse() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.deleteResponse = &proto.DeleteResponse{}
}

func (c *TestClient) OverrideMultiSetResponse(failedKeys []string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.multiSetResponse = &proto.MultiSetResponse{FailedKeys: failedKeys}
}

// Not used by tests, but you may use it in your own tests
func (c *TestClient) OverrideGetShardContentsResponse(response *proto.GetShardContentsResponse) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.getShardContentsResponse = response
}

func (c *TestClient) SetLatencyInjection(duration time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.latencyInjection = &duration
}

func (c *TestClient) AppendList(ctx context.Context, req *proto.AppendListRequest, opts ...grpc.CallOption) (*proto.AppendListResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.appendListResponse, nil
	}
	return c.server.AppendList(ctx, req)
}

func (c *TestClient) CreateList(ctx context.Context, req *proto.CreateListRequest, opts ...grpc.CallOption) (*proto.CreateListResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.createListResponse, nil
	}
	return c.server.CreateList(ctx, req)
}

func (c *TestClient) CreateSet(ctx context.Context, req *proto.CreateSetRequest, opts ...grpc.CallOption) (*proto.CreateSetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.createSetResponse, nil
	}
	return c.server.CreateSet(ctx, req)
}

func (c *TestClient) CreateSortedSet(ctx context.Context, req *proto.CreateSortedSetRequest, opts ...grpc.CallOption) (*proto.CreateSortedSetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.createSortedSetResponse, nil
	}
	return c.server.CreateSortedSet(ctx, req)
}

func (c *TestClient) AppendSet(ctx context.Context, req *proto.AppendSetRequest, opts ...grpc.CallOption) (*proto.AppendSetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.appendSetResponse, nil
	}
	return c.server.AppendSet(ctx, req)
}

func (c *TestClient) AppendSortedSet(ctx context.Context, req *proto.AppendSortedSetRequest, opts ...grpc.CallOption) (*proto.AppendSortedSetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.appendSortedSetResponse, nil
	}
	return c.server.AppendSortedSet(ctx, req)
}

func (c *TestClient) PopList(ctx context.Context, req *proto.PopListRequest, opts ...grpc.CallOption) (*proto.PopListResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.popListResponse, nil
	}
	return c.server.PopList(ctx, req)
}

func (c *TestClient) RemoveList(ctx context.Context, req *proto.RemoveListRequest, opts ...grpc.CallOption) (*proto.RemoveListResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.removeListResponse, nil
	}
	return c.server.RemoveList(ctx, req)
}

func (c *TestClient) RemoveSet(ctx context.Context, req *proto.RemoveSetRequest, opts ...grpc.CallOption) (*proto.RemoveSetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.removeSetResponse, nil
	}
	return c.server.RemoveSet(ctx, req)
}

func (c *TestClient) RemoveSortedSet(ctx context.Context, req *proto.RemoveSortedSetRequest, opts ...grpc.CallOption) (*proto.RemoveSortedSetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.removeSortedSetResponse, nil
	}
	return c.server.RemoveSortedSet(ctx, req)
}

func (c *TestClient) CheckList(ctx context.Context, req *proto.CheckListRequest, opts ...grpc.CallOption) (*proto.CheckListResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.checkListResponse, nil
	}
	return c.server.CheckList(ctx, req)
}

func (c *TestClient) CheckSet(ctx context.Context, req *proto.CheckSetRequest, opts ...grpc.CallOption) (*proto.CheckSetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.checkSetResponse, nil
	}
	return c.server.CheckSet(ctx, req)
}

func (c *TestClient) CheckSortedSet(ctx context.Context, req *proto.CheckSortedSetRequest, opts ...grpc.CallOption) (*proto.CheckSortedSetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.checkSortedSetResponse, nil
	}
	return c.server.CheckSortedSet(ctx, req)
}

func (c *TestClient) MultiSet(ctx context.Context, req *proto.MultiSetRequest, opts ...grpc.CallOption) (*proto.MultiSetResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.multiSetResponse != nil {
		return c.multiSetResponse, nil
	}
	log.Printf("server %p", c.server)
	return c.server.MultiSet(ctx, req)
}

func (c *TestClient) CAS(ctx context.Context, req *proto.CASRequest, opts ...grpc.CallOption) (*proto.CASResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.cASResponse, nil
	}
	return c.server.CAS(ctx, req)
}

func (c *TestClient) GetRange(ctx context.Context, req *proto.GetRangeRequest, opts ...grpc.CallOption) (*proto.GetRangeResponse, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	atomic.AddUint64(&c.requestsSent, 1)
	if c.err != nil {
		return nil, c.err
	}
	if c.latencyInjection != nil {
		time.Sleep(*c.latencyInjection)
	}
	if c.getShardContentsResponse != nil {
		return c.getRangeResponse, nil
	}
	return c.server.GetRange(ctx, req)
}
