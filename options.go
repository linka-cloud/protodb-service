package service

import (
	"context"

	"go.linka.cloud/protodb/typed"
)

type Option[T any, R Resource[T]] func(s *options[T, R])

func WithHooks[T any, R Resource[T]](hooks ...any) Option[T, R] {
	return func(o *options[T, R]) {
		for _, v := range hooks {
			if h, ok := v.(BeforeCreateHook[T, R]); ok {
				o.beforeCreate = append(o.beforeCreate, h)
			}
			if h, ok := v.(AfterCreateHook[T, R]); ok {
				o.afterCreate = append(o.afterCreate, h)
			}
			if h, ok := v.(AfterReadHook[T, R]); ok {
				o.afterRead = append(o.afterRead, h)
			}
			if h, ok := v.(BeforeUpdateHook[T, R]); ok {
				o.beforeUpdate = append(o.beforeUpdate, h)
			}
			if h, ok := v.(AfterUpdateHook[T, R]); ok {
				o.afterUpdate = append(o.afterUpdate, h)
			}
			if h, ok := v.(BeforeDeleteHook[T, R]); ok {
				o.beforeDelete = append(o.beforeDelete, h)
			}
			if h, ok := v.(AfterDeleteHook[T, R]); ok {
				o.afterDelete = append(o.afterDelete, h)
			}
			if h, ok := v.(AfterListHook[T, R]); ok {
				o.afterList = append(o.afterList, h)
			}
			if h, ok := v.(BeforeEmit[T, R]); ok {
				o.beforeEmit = append(o.beforeEmit, h)
			}
		}
	}
}

func WithBeforeCreate[T any, R Resource[T]](fn func(ctx context.Context, tx typed.Tx[T, R], resource R) (R, error)) Option[T, R] {
	return func(o *options[T, R]) {
		o.beforeCreate = append(o.beforeCreate, &beforeCreate[T, R]{fn: fn})
	}
}

func WithAfterCreate[T any, R Resource[T]](fn func(ctx context.Context, tx typed.Tx[T, R], resource R) (R, error)) Option[T, R] {
	return func(o *options[T, R]) {
		o.afterCreate = append(o.afterCreate, &afterCreate[T, R]{fn: fn})
	}
}

func WithAfterRead[T any, R Resource[T]](fn func(ctx context.Context, tx typed.Tx[T, R], resource R) (R, error)) Option[T, R] {
	return func(o *options[T, R]) {
		o.afterRead = append(o.afterRead, &afterRead[T, R]{fn: fn})
	}
}

func WithBeforeUpdate[T any, R Resource[T]](fn func(ctx context.Context, tx typed.Tx[T, R], old, new R) (R, error)) Option[T, R] {
	return func(o *options[T, R]) {
		o.beforeUpdate = append(o.beforeUpdate, &beforeUpdate[T, R]{fn: fn})
	}
}

func WithAfterUpdate[T any, R Resource[T]](fn func(ctx context.Context, tx typed.Tx[T, R], old, new R) (R, error)) Option[T, R] {
	return func(o *options[T, R]) {
		o.afterUpdate = append(o.afterUpdate, &afterUpdate[T, R]{fn: fn})
	}
}

func WithBeforeDelete[T any, R Resource[T]](fn func(ctx context.Context, tx typed.Tx[T, R], resource R) (bool, error)) Option[T, R] {
	return func(o *options[T, R]) {
		o.beforeDelete = append(o.beforeDelete, &beforeDelete[T, R]{fn: fn})
	}
}

func WithAfterDelete[T any, R Resource[T]](fn func(ctx context.Context, tx typed.Tx[T, R], resource R) error) Option[T, R] {
	return func(o *options[T, R]) {
		o.afterDelete = append(o.afterDelete, &afterDelete[T, R]{fn: fn})
	}
}

func WithAfterList[T any, R Resource[T]](fn func(ctx context.Context, tx typed.Tx[T, R], resources []R) ([]R, error)) Option[T, R] {
	return func(o *options[T, R]) {
		o.afterList = append(o.afterList, &afterList[T, R]{fn: fn})
	}
}

func WithBeforeEmit[T any, R Resource[T]](fn func(ctx context.Context, store typed.Store[T, R], ev *Event[T, R]) (*Event[T, R], error)) Option[T, R] {
	return func(o *options[T, R]) {
		o.beforeEmit = append(o.beforeEmit, &beforeEmit[T, R]{fn: fn})
	}
}

type options[T any, R Resource[T]] struct {
	beforeCreate []BeforeCreateHook[T, R]
	afterCreate  []AfterCreateHook[T, R]
	beforeUpdate []BeforeUpdateHook[T, R]
	afterUpdate  []AfterUpdateHook[T, R]
	beforeDelete []BeforeDeleteHook[T, R]
	afterDelete  []AfterDeleteHook[T, R]
	afterRead    []AfterReadHook[T, R]
	afterList    []AfterListHook[T, R]
	beforeEmit   []BeforeEmit[T, R]
}

func (o *options[T, R]) BeforeCreate(ctx context.Context, tx typed.Tx[T, R], resource R, opts ...Option[T, R]) (R, error) {
	var err error
	for _, v := range o.merge(opts...).beforeCreate {
		if resource, err = v.BeforeCreate(ctx, tx, resource); err != nil {
			return resource, err
		}
	}
	return resource, nil
}

func (o *options[T, R]) AfterCreate(ctx context.Context, tx typed.Tx[T, R], resource R, opts ...Option[T, R]) (R, error) {
	var err error
	for _, v := range o.merge(opts...).afterCreate {
		if resource, err = v.AfterCreate(ctx, tx, resource); err != nil {
			return resource, err
		}
	}
	return resource, nil
}

func (o *options[T, R]) AfterRead(ctx context.Context, tx typed.Tx[T, R], resource R, opts ...Option[T, R]) (R, error) {
	var err error
	for _, v := range o.merge(opts...).afterRead {
		if resource, err = v.AfterRead(ctx, tx, resource); err != nil {
			return resource, err
		}
	}
	return resource, nil
}

func (o *options[T, R]) BeforeUpdate(ctx context.Context, tx typed.Tx[T, R], old, new R, opts ...Option[T, R]) (R, error) {
	var err error
	for _, v := range o.merge(opts...).beforeUpdate {
		if new, err = v.BeforeUpdate(ctx, tx, old, new); err != nil {
			return new, err
		}
	}
	return new, nil
}

func (o *options[T, R]) AfterUpdate(ctx context.Context, tx typed.Tx[T, R], old, new R, opts ...Option[T, R]) (R, error) {
	var err error
	for _, v := range o.merge(opts...).afterUpdate {
		if new, err = v.AfterUpdate(ctx, tx, old, new); err != nil {
			return new, err
		}
	}
	return new, nil
}

func (o *options[T, R]) BeforeDelete(ctx context.Context, tx typed.Tx[T, R], resource R, opts ...Option[T, R]) (bool, error) {
	var err error
	for _, v := range o.merge(opts...).beforeDelete {
		var proceed bool
		if proceed, err = v.BeforeDelete(ctx, tx, resource); err != nil {
			return false, err
		}
		if !proceed {
			return false, nil
		}
	}
	return true, nil
}

func (o *options[T, R]) AfterDelete(ctx context.Context, tx typed.Tx[T, R], resource R, opts ...Option[T, R]) error {
	var err error
	for _, v := range o.merge(opts...).afterDelete {
		if err = v.AfterDelete(ctx, tx, resource); err != nil {
			return err
		}
	}
	return nil
}

func (o *options[T, R]) AfterList(ctx context.Context, tx typed.Tx[T, R], resources []R, opts ...Option[T, R]) ([]R, error) {
	var err error
	for _, v := range o.merge(opts...).afterList {
		if resources, err = v.AfterList(ctx, tx, resources); err != nil {
			return resources, err
		}
	}
	return resources, nil
}

func (o *options[T, R]) BeforeEmit(ctx context.Context, store typed.Store[T, R], ev *Event[T, R], opts ...Option[T, R]) (*Event[T, R], error) {
	var err error
	for _, v := range o.merge(opts...).beforeEmit {
		if ev, err = v.BeforeEmit(ctx, store, ev); err != nil {
			return ev, err
		}
	}
	return ev, nil
}

func (o *options[T, R]) merge(opts ...Option[T, R]) *options[T, R] {
	o2 := &options[T, R]{}
	o2.beforeCreate = append(o2.beforeCreate, o.beforeCreate...)
	o2.afterCreate = append(o2.afterCreate, o.afterCreate...)
	o2.beforeUpdate = append(o2.beforeUpdate, o.beforeUpdate...)
	o2.afterUpdate = append(o2.afterUpdate, o.afterUpdate...)
	o2.beforeDelete = append(o2.beforeDelete, o.beforeDelete...)
	o2.afterDelete = append(o2.afterDelete, o.afterDelete...)
	o2.afterRead = append(o2.afterRead, o.afterRead...)
	o2.afterList = append(o2.afterList, o.afterList...)
	o2.beforeEmit = append(o2.beforeEmit, o.beforeEmit...)
	for _, v := range opts {
		v(o2)
	}
	return o2
}
