package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, err
	}

	resp := &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: val == nil,
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	batch := storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}

	err := server.storage.Write(req.Context, []storage.Modify{batch})
	if err != nil {
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	batch := storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}

	err := server.storage.Write(req.Context, []storage.Modify{batch})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{
			Error: err.Error(),
		}, err
	}
	defer reader.Close()

	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	defer iter.Close()

	var res []*kvrpcpb.KvPair
	for limit := req.Limit; limit > 0 && iter.Valid(); {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			return &kvrpcpb.RawScanResponse{
				Error: err.Error(),
			}, err
		}
		res = append(res, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		})

		iter.Next()
		limit--
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: res,
	}, nil
}
