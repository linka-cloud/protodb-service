package main

import (
	"context"
	"fmt"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/google/uuid"
	gerrs "go.linka.cloud/grpc-toolkit/errors"
	"go.linka.cloud/grpc-toolkit/logger"
	"go.linka.cloud/protodb"
	"go.linka.cloud/protodb/typed"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pdbsvc "go.linka.cloud/protodb-service"
	"go.linka.cloud/protodb-service/tests/pb"

	pb "go.linka.cloud/protodb-service/tests/pb"
)

//go:generate protoc -I. -I/adphi/proto --go-patch_out=plugin=go,paths=source_relative:. --go-patch_out=plugin=go-grpc,paths=source_relative:. --go-patch_out=plugin=go-vtproto,paths=source_relative,features=marshal+unmarshal+size+equal+clone:. --go-patch_out=plugin=protodb-service,paths=source_relative:. pb/types.proto pb/resource_service.proto
// //go:generate protoc -I. -I/adphi/proto --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. --go-vtproto_out=paths=source_relative,features=marshal+unmarshal+size+equal+clone:. --protodb-service_out=paths=source_relative:. pb/pb.proto

var (
	_ pdbsvc.BeforeCreateHook[pb.Resource, *pb.Resource] = (*md)(nil)
	_ pdbsvc.BeforeUpdateHook[pb.Resource, *pb.Resource] = (*md)(nil)
	_ pdbsvc.BeforeCreateHook[pb.Resource, *pb.Resource] = (*unique)(nil)
	_ pdbsvc.BeforeUpdateHook[pb.Resource, *pb.Resource] = (*unique)(nil)
	_ pdbsvc.AfterReadHook[pb.Resource, *pb.Resource]    = (*softDelete)(nil)
	_ pdbsvc.AfterListHook[pb.Resource, *pb.Resource]    = (*softDelete)(nil)
	_ pdbsvc.BeforeUpdateHook[pb.Resource, *pb.Resource] = (*softDelete)(nil)
	_ pdbsvc.BeforeDeleteHook[pb.Resource, *pb.Resource] = (*softDelete)(nil)
	_ pdbsvc.BeforeDeleteHook[pb.Resource, *pb.Resource] = (*finalizer)(nil)
)

type md struct{}

func (md) BeforeCreate(ctx context.Context, tx typed.Tx[pb.Resource, *pb.Resource], resource *pb.Resource) (*pb.Resource, error) {
	if resource.Metadata == nil {
		resource.Metadata = &pb.Metadata{}
	}
	resource.ID = uuid.NewString()
	resource.Metadata.CreatedAt = timestamppb.Now()
	return resource, nil
}

func (md) BeforeUpdate(ctx context.Context, tx typed.Tx[pb.Resource, *pb.Resource], old, new *pb.Resource) (*pb.Resource, error) {
	if new.Metadata == nil {
		new.Metadata = &pb.Metadata{}
	}
	new.Metadata.CreatedAt = old.Metadata.CreatedAt
	new.Metadata.UpdatedAt = timestamppb.Now()
	return new, nil
}

type unique struct{}

func (u unique) BeforeCreate(ctx context.Context, tx typed.Tx[pb.Resource, *pb.Resource], resource *pb.Resource) (*pb.Resource, error) {
	if err := u.unique(ctx, tx, resource); err != nil {
		return nil, err
	}
	return resource, nil
}

func (u unique) BeforeUpdate(ctx context.Context, tx typed.Tx[pb.Resource, *pb.Resource], _, new *pb.Resource) (*pb.Resource, error) {
	if err := u.unique(ctx, tx, new); err != nil {
		return nil, err
	}
	return new, nil
}

func (unique) unique(ctx context.Context, tx typed.Tx[pb.Resource, *pb.Resource], resource *pb.Resource) error {
	_, ok, err := tx.GetOne(ctx, &pb.Resource{}, protodb.WithFilter(protodb.Where("name").StringEquals(resource.Name).And("id").StringNotEquals(resource.ID)))
	if err != nil {
		return err
	}
	if ok {
		return gerrs.FailedPreconditionf("resource with name %q already exists", resource.Name)
	}
	return nil
}

type softDelete struct{}

func (softDelete) AfterRead(ctx context.Context, tx typed.Tx[pb.Resource, *pb.Resource], resource *pb.Resource) (*pb.Resource, error) {
	if resource.GetMetadata().GetDeletedAt() != nil {
		return nil, gerrs.NotFoundf("not found")
	}
	return resource, nil
}

func (softDelete) AfterList(ctx context.Context, tx typed.Tx[pb.Resource, *pb.Resource], resources []*pb.Resource) ([]*pb.Resource, error) {
	var filtered []*pb.Resource
	for _, r := range resources {
		if r.GetMetadata().GetDeletedAt() == nil {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

func (softDelete) BeforeUpdate(ctx context.Context, tx typed.Tx[pb.Resource, *pb.Resource], old, new *pb.Resource) (*pb.Resource, error) {
	if old.GetMetadata().GetDeletedAt() != nil {
		return nil, gerrs.NotFoundf("not found")
	}
	return new, nil
}

func (softDelete) BeforeDelete(ctx context.Context, tx typed.Tx[pb.Resource, *pb.Resource], resource *pb.Resource) (bool, error) {
	if resource.GetMetadata().GetDeletedAt() != nil {
		return true, nil
	}
	resource.Metadata.DeletedAt = timestamppb.Now()
	_, err := tx.Set(ctx, resource)
	return false, err
}

type finalizer struct{}

func (finalizer) BeforeDelete(ctx context.Context, tx typed.Tx[pb.Resource, *pb.Resource], resource *pb.Resource) (bool, error) {
	if len(resource.GetMetadata().GetFinalizers()) == 0 {
		return true, nil
	}
	if resource.Metadata == nil {
		resource.Metadata = &pb.Metadata{}
	}
	if resource.Metadata.DeletedAt == nil {
		resource.Metadata.DeletedAt = timestamppb.Now()
	}
	_, err := tx.Set(ctx, resource)
	return false, err
}

type svc struct {
	*pb.DefaultService
}

func (s *svc) AddLabels(ctx context.Context, req *pb.AddLabelsRequest) (*pb.AddLabelsResponse, error) {
	res, err := s.Service.Update(ctx, &pb.Resource{ID: req.ID}, nil, pdbsvc.WithBeforeUpdate(func(ctx context.Context, tx typed.Tx[pb.Resource, *pb.Resource], old, new *pb.Resource) (*pb.Resource, error) {
		if old.Metadata == nil {
			old.Metadata = &pb.Metadata{}
		}
		if old.Metadata.Labels == nil {
			old.Metadata.Labels = make(map[string]string)
		}
		for _, v := range req.Labels {
			old.Metadata.Labels[v.Key] = v.Value
		}
		return old, nil
	}))
	if err != nil {
		return nil, err
	}
	return &pb.AddLabelsResponse{Resource: res}, nil
}

func (s *svc) RemoveLabels(ctx context.Context, req *pb.RemoveLabelsRequest) (*pb.RemoveLabelsResponse, error) {
	res, err := s.Service.Update(ctx, &pb.Resource{ID: req.ID}, nil, pdbsvc.WithBeforeUpdate(func(ctx context.Context, tx typed.Tx[pb.Resource, *pb.Resource], old, new *pb.Resource) (*pb.Resource, error) {
		if old.Metadata == nil || old.Metadata.Labels == nil {
			return old, nil
		}
		for _, k := range req.Keys {
			delete(old.Metadata.Labels, k)
		}
		return old, nil
	}))
	if err != nil {
		return nil, err
	}
	return &pb.RemoveLabelsResponse{Resource: res}, nil
}

func run(ctx context.Context) error {
	db, err := protodb.Open(ctx, protodb.WithInMemory(true), protodb.WithLogger(logger.C(ctx).WithOffset(2)), protodb.WithIgnoreProtoRegisterErrors())
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	s := &svc{pb.NewDefaultService(db, pdbsvc.WithHooks[resource.Resource](unique{}, md{}, finalizer{}))}

	ch := &inprocgrpc.Channel{}
	pb.RegisterResourceServiceServer(ch, s)
	c := pb.NewResourceServiceClient(ch)

	w, err := c.Watch(ctx, &resource.WatchRequest{})
	if err != nil {
		return fmt.Errorf("start watch: %w", err)
	}
	defer w.CloseSend()
	ready := make(chan struct{})
	go func() {
		close(ready)
		for {
			e, err := w.Recv()
			if err != nil {
				return
			}
			fmt.Printf("Watch Event: %+v\n", e)
		}
	}()
	<-ready
	// Create Resource
	createRes, err := c.Create(ctx, &resource.CreateRequest{
		Resource: &pb.Resource{
			Name: "Test Resource",
		},
	})
	if err != nil {
		return fmt.Errorf("create resource: %w", err)
	}
	fmt.Printf("Created Resource: %v\n", createRes.Resource)

	// Read Resource
	readRes, err := c.Read(ctx, &resource.ReadRequest{
		ID: createRes.Resource.ID,
	})
	if err != nil {
		return fmt.Errorf("read resource: %w", err)
	}
	fmt.Printf("Read Resource: %v\n", readRes.Resource)

	_, err = c.Create(ctx, &resource.CreateRequest{
		Resource: &pb.Resource{
			Name: readRes.Resource.Name,
		},
	})
	if err == nil {
		return fmt.Errorf("expected error when creating resource with duplicate name")
	}

	// Update Resource
	updateRes, err := c.Update(ctx, &resource.UpdateRequest{
		Resource: &pb.Resource{
			ID:   createRes.Resource.ID,
			Name: "Updated Resource",
			Metadata: &pb.Metadata{
				Finalizers: []string{"protect-from-deletion"},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("update resource: %w", err)
	}
	fmt.Printf("Updated Resource: %v\n", updateRes.Resource)

	addLabelsRes, err := c.AddLabels(ctx, &pb.AddLabelsRequest{ID: createRes.Resource.ID, Labels: []*pb.Label{{Key: "env", Value: "prod"}, {Key: "team", Value: "devops"}}})
	if err != nil {
		return fmt.Errorf("add labels: %w", err)
	}
	if len(addLabelsRes.Resource.Metadata.Labels) != 2 {
		return fmt.Errorf("expected 2 labels, got %d", len(addLabelsRes.Resource.Metadata.Labels))
	}
	fmt.Printf("Added Labels: %v\n", addLabelsRes.Resource.Metadata.Labels)

	removeLabelsRes, err := c.RemoveLabels(ctx, &pb.RemoveLabelsRequest{ID: createRes.Resource.ID, Keys: []string{"env"}})
	if err != nil {
		return fmt.Errorf("remove labels: %w", err)
	}
	if len(removeLabelsRes.Resource.Metadata.Labels) != 1 {
		return fmt.Errorf("expected 1 label, got %d", len(removeLabelsRes.Resource.Metadata.Labels))
	}
	fmt.Printf("Removed Labels: %v\n", removeLabelsRes.Resource.Metadata.Labels)

	// Delete Resource
	_, err = c.Delete(ctx, &resource.DeleteRequest{
		ID: createRes.Resource.ID,
	})
	if err != nil {
		return fmt.Errorf("delete resource: %w", err)
	}
	fmt.Println("Soft Deleted Resource")
	readRes, err = c.Read(ctx, &resource.ReadRequest{
		ID: createRes.Resource.ID,
	})
	if err != nil {
		return fmt.Errorf("read resource after soft delete: %w", err)
	}
	fmt.Printf("Read Resource after Soft Delete: %v\n", readRes.Resource)

	// Remove Finalizers
	updateRes, err = c.Update(ctx, &resource.UpdateRequest{
		Resource: &pb.Resource{
			ID: createRes.Resource.ID,
		},
		Fields: &fieldmaskpb.FieldMask{Paths: []string{"metadata.finalizers"}},
	})

	// Hard Delete Resource
	_, err = c.Delete(ctx, &resource.DeleteRequest{
		ID: createRes.Resource.ID,
	})
	if err != nil {
		return fmt.Errorf("hard delete resource: %w", err)
	}
	fmt.Println("Hard Deleted Resource")
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := run(ctx); err != nil {
		panic(err)
	}
}
