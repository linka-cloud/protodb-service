package service

import (
	"context"

	gerrs "go.linka.cloud/grpc-toolkit/errors"
	"go.linka.cloud/protodb"
	"go.linka.cloud/protodb/typed"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type EventType int32

const (
	EventTypeUnknown EventType = 0
	EventTypeEnter   EventType = 1
	EventTypeLeave   EventType = 2
	EventTypeUpdate  EventType = 3
)

type Event[T any, R Resource[T]] struct {
	Type EventType
	Old  R
	New  R
}

type Service[T any, R Resource[T]] interface {
	Create(context.Context, R, ...Option[T, R]) (R, error)
	Read(context.Context, string, *fieldmaskpb.FieldMask, ...Option[T, R]) (R, error)
	Update(context.Context, R, *fieldmaskpb.FieldMask, ...Option[T, R]) (R, error)
	Delete(context.Context, string, ...Option[T, R]) error
	List(context.Context, protodb.Filter, *fieldmaskpb.FieldMask, *protodb.Paging, ...Option[T, R]) ([]R, *protodb.PagingInfo, error)
	Watch(protodb.Filter, *fieldmaskpb.FieldMask, grpc.ServerStreamingServer[Event[T, R]], ...Option[T, R]) error
}

func New[T any, R Resource[T]](store protodb.DB, opts ...Option[T, R]) Service[T, R] {
	o := &options[T, R]{}
	for _, v := range opts {
		v(o)
	}
	return &service[T, R]{store: store, opts: o}
}

type service[T any, R Resource[T]] struct {
	store protodb.DB
	opts  *options[T, R]
}

func (s *service[T, R]) Create(ctx context.Context, r R, opts ...Option[T, R]) (res R, err error) {
	return typed.WithTypedTx2[T, R](ctx, s.store, func(ctx context.Context, tx typed.Tx[T, R]) (R, error) {
		if r, err = s.opts.BeforeCreate(ctx, tx, r, opts...); err != nil {
			return nil, err
		}
		r, err := tx.Set(ctx, r)
		if err != nil {
			return nil, gerrs.Internal(err)
		}
		if r, err = s.opts.AfterCreate(ctx, tx, r, opts...); err != nil {
			return nil, err
		}
		return r, nil
	})
}

func (s *service[T, R]) Read(ctx context.Context, id string, fields *fieldmaskpb.FieldMask, opts ...Option[T, R]) (res R, err error) {
	return typed.WithTypedTx2[T, R](ctx, s.store, func(ctx context.Context, tx typed.Tx[T, R]) (R, error) {
		var z T
		r, ok, err := tx.GetOne(ctx, &z, protodb.WithFilter(protodb.Where("id").StringEquals(id)), protodb.WithReadFieldMask(fields))
		if err != nil {
			return nil, gerrs.Internal(err)
		}
		if !ok {
			return nil, gerrs.NotFoundf("not found")
		}
		if r, err = s.opts.AfterRead(ctx, tx, r, opts...); err != nil {
			return nil, err
		}
		return r, nil
	})
}

func (s *service[T, R]) Update(ctx context.Context, r R, fields *fieldmaskpb.FieldMask, opts ...Option[T, R]) (res R, err error) {
	return typed.WithTypedTx2[T, R](ctx, s.store, func(ctx context.Context, tx typed.Tx[T, R]) (R, error) {
		old, ok, err := tx.GetOne(ctx, r)
		if err != nil {
			return nil, gerrs.Internal(err)
		}
		if !ok {
			return nil, gerrs.NotFoundf("not found")
		}
		if r, err = s.opts.BeforeUpdate(ctx, tx, old, r, opts...); err != nil {
			return nil, err
		}
		r, err := tx.Set(ctx, r, protodb.WithWriteFieldMask(fields))
		if err != nil {
			return nil, gerrs.Internal(err)
		}
		if r, err = s.opts.AfterUpdate(ctx, tx, old, r, opts...); err != nil {
			return nil, err
		}

		return r, nil
	})
}

func (s *service[T, R]) Delete(ctx context.Context, id string, opts ...Option[T, R]) (err error) {
	return typed.WithTypedTx[T, R](ctx, s.store, func(ctx context.Context, tx typed.Tx[T, R]) error {
		var z T
		r, ok, err := tx.GetOne(ctx, &z, protodb.WithFilter(protodb.Where("id").StringEquals(id)))
		if err != nil {
			return gerrs.Internal(err)
		}
		if !ok {
			return gerrs.NotFoundf("not found")
		}
		if ok, err = s.opts.BeforeDelete(ctx, tx, r, opts...); !ok || err != nil {
			return err
		}
		if err := tx.Delete(ctx, r); err != nil {
			return gerrs.Internal(err)
		}
		if err = s.opts.AfterDelete(ctx, tx, r, opts...); err != nil {
			return err
		}
		return nil
	})
}

func (s *service[T, R]) List(ctx context.Context, filter protodb.Filter, fields *fieldmaskpb.FieldMask, paging *protodb.Paging, opts ...Option[T, R]) (res []R, info *protodb.PagingInfo, err error) {
	return withTypedTx3(ctx, s.store, func(ctx context.Context, tx typed.Tx[T, R]) ([]R, *protodb.PagingInfo, error) {
		var z T
		rs, i, err := tx.Get(ctx, &z, protodb.WithFilter(filter.Expr()), protodb.WithReadFieldMask(fields), protodb.WithPaging(paging))
		if err != nil {
			return nil, nil, gerrs.Internal(err)
		}
		if rs, err = s.opts.AfterList(ctx, tx, rs, opts...); err != nil {
			return nil, nil, err
		}
		return rs, i, nil
	})
}

func (s *service[T, R]) Watch(filter protodb.Filter, fields *fieldmaskpb.FieldMask, ss grpc.ServerStreamingServer[Event[T, R]], opts ...Option[T, R]) error {
	var z T
	store := typed.NewStore[T, R](s.store)
	w, err := store.Watch(ss.Context(), &z, protodb.WithFilter(filter.Expr()), protodb.WithReadFieldMask(fields))
	if err != nil {
		return gerrs.Internal(err)
	}
	for ev := range w {
		if err = ev.Err(); err != nil {
			return gerrs.Internal(err)
		}
		e := &Event[T, R]{
			Type: EventType(ev.Type()),
			Old:  ev.Old(),
			New:  ev.New(),
		}
		e, err = s.opts.BeforeEmit(ss.Context(), store, e, opts...)
		if err != nil {
			return err
		}
		if err := ss.Send(e); err != nil {
			return err
		}
	}
	return nil
}
