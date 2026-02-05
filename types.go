package service

import (
	"context"

	"go.linka.cloud/protodb"
	"go.linka.cloud/protodb/typed"
	"google.golang.org/protobuf/proto"
)

type Message[T any] interface {
	proto.Message
}

type Resource[T any] interface {
	Message[T]
	*T
}

type BeforeCreateHook[T any, R Resource[T]] interface {
	BeforeCreate(ctx context.Context, tx typed.Tx[T, R], resource R) (R, error)
}

type AfterCreateHook[T any, R Resource[T]] interface {
	AfterCreate(ctx context.Context, tx typed.Tx[T, R], resource R) (R, error)
}

type AfterReadHook[T any, R Resource[T]] interface {
	AfterRead(ctx context.Context, tx typed.Tx[T, R], resource R) (R, error)
}

type BeforeUpdateHook[T any, R Resource[T]] interface {
	BeforeUpdate(ctx context.Context, tx typed.Tx[T, R], old, new R) (R, error)
}

type AfterUpdateHook[T any, R Resource[T]] interface {
	AfterUpdate(ctx context.Context, tx typed.Tx[T, R], old, new R) (R, error)
}

type BeforeDeleteHook[T any, R Resource[T]] interface {
	BeforeDelete(ctx context.Context, tx typed.Tx[T, R], resource R) (bool, error)
}

type AfterDeleteHook[T any, R Resource[T]] interface {
	AfterDelete(ctx context.Context, tx typed.Tx[T, R], resource R) error
}

type AfterListHook[T any, R Resource[T]] interface {
	AfterList(ctx context.Context, tx typed.Tx[T, R], resources []R) ([]R, error)
}

type BeforeEmit[T any, R Resource[T]] interface {
	BeforeEmit(ctx context.Context, store typed.Store[T, R], ev *Event[T, R]) (*Event[T, R], error)
}

type beforeCreate[T any, R Resource[T]] struct {
	fn func(ctx context.Context, tx typed.Tx[T, R], resource R) (R, error)
}

func (b beforeCreate[T, R]) BeforeCreate(ctx context.Context, tx typed.Tx[T, R], resource R) (R, error) {
	return b.fn(ctx, tx, resource)
}

type afterCreate[T any, R Resource[T]] struct {
	fn func(ctx context.Context, tx typed.Tx[T, R], resource R) (R, error)
}

func (a afterCreate[T, R]) AfterCreate(ctx context.Context, tx typed.Tx[T, R], resource R) (R, error) {
	return a.fn(ctx, tx, resource)
}

type afterRead[T any, R Resource[T]] struct {
	fn func(ctx context.Context, tx typed.Tx[T, R], resource R) (R, error)
}

func (a afterRead[T, R]) AfterRead(ctx context.Context, tx typed.Tx[T, R], resource R) (R, error) {
	return a.fn(ctx, tx, resource)
}

type beforeUpdate[T any, R Resource[T]] struct {
	fn func(ctx context.Context, tx typed.Tx[T, R], old, new R) (R, error)
}

func (b beforeUpdate[T, R]) BeforeUpdate(ctx context.Context, tx typed.Tx[T, R], old, new R) (R, error) {
	return b.fn(ctx, tx, old, new)
}

type afterUpdate[T any, R Resource[T]] struct {
	fn func(ctx context.Context, tx typed.Tx[T, R], old, new R) (R, error)
}

func (a afterUpdate[T, R]) AfterUpdate(ctx context.Context, tx typed.Tx[T, R], old, new R) (R, error) {
	return a.fn(ctx, tx, old, new)
}

type beforeDelete[T any, R Resource[T]] struct {
	fn func(ctx context.Context, tx typed.Tx[T, R], resource R) (bool, error)
}

func (b beforeDelete[T, R]) BeforeDelete(ctx context.Context, tx typed.Tx[T, R], resource R) (bool, error) {
	return b.fn(ctx, tx, resource)
}

type afterDelete[T any, R Resource[T]] struct {
	fn func(ctx context.Context, tx typed.Tx[T, R], resource R) error
}

func (a afterDelete[T, R]) AfterDelete(ctx context.Context, tx typed.Tx[T, R], resource R) error {
	return a.fn(ctx, tx, resource)
}

type afterList[T any, R Resource[T]] struct {
	fn func(ctx context.Context, tx typed.Tx[T, R], resources []R) ([]R, error)
}

func (a afterList[T, R]) AfterList(ctx context.Context, tx typed.Tx[T, R], resources []R) ([]R, error) {
	return a.fn(ctx, tx, resources)
}

type beforeEmit[T any, R Resource[T]] struct {
	fn func(ctx context.Context, store typed.Store[T, R], ev *Event[T, R]) (*Event[T, R], error)
}

func (b beforeEmit[T, R]) BeforeEmit(ctx context.Context, store typed.Store[T, R], ev *Event[T, R]) (*Event[T, R], error) {
	return b.fn(ctx, store, ev)
}

func withTypedTx3[T any, PT Resource[T], R any, R2 any](ctx context.Context, db protodb.TxProvider, fn func(ctx context.Context, tx typed.Tx[T, PT]) (R, R2, error), opts ...protodb.TxOption) (R, R2, error) {
	var (
		o  R
		o2 R2
	)
	tx, err := db.Tx(ctx, opts...)
	if err != nil {
		return o, o2, err
	}
	defer tx.Close()
	o, o2, err = fn(ctx, typed.NewTx[T, PT](tx))
	if err != nil {
		return o, o2, err
	}
	if err := tx.Commit(ctx); err != nil {
		return o, o2, err
	}
	return o, o2, nil
}
