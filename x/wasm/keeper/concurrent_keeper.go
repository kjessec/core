package keeper

import (
	"context"
	"fmt"
	wasmvm "github.com/CosmWasm/wasmvm"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/terra-money/core/x/wasm/config"
	"github.com/terra-money/core/x/wasm/types"
	"path/filepath"
	"sync/atomic"
)

const (
	contextKeyExecutionType    = "contextKeyExecutionType"
	contextKeyAllocatedVMIndex = "contextKeyAllocatedVMIndex"
)

const (
	ExecutionTypeExecution = iota
	ExecutionTypeQuery
)

type ExecutionType int

type ConcurrentWasmVMConfig struct {
	// NumParallelism defines how many vm's to spawn
	NumParallelism uint

	// NumWorkersPerVM unused
	NumWorkersPerVM uint
}

type ConcurrentWasmVMContext struct {
	i                 uint64
	config            ConcurrentWasmVMConfig
	concurrentWasmVMs []types.WasmerEngine
}

func NewConcurrentWasmVMContext(
	supportedFeatures string,
	homePath string,
	wasmConfig *config.Config,
	concurrencyFactor int,
) (*ConcurrentWasmVMContext, error) {
	wasmVMs := make([]types.WasmerEngine, concurrencyFactor)

	if concurrencyFactor < 0 {
		panic(types.ErrInvalidConcurrencyFactor)
	}

	for i := 0; i < concurrencyFactor; i++ {
		vm, err := wasmvm.NewVM(
			filepath.Join(homePath, config.DBDir),
			supportedFeatures,
			types.ContractMemoryLimit,
			wasmConfig.ContractDebugMode,
			wasmConfig.ContractMemoryCacheSize,
			wasmConfig.RefreshThreadNum,
		)

		if err != nil {
			return nil, err
		}

		wasmVMs[i] = vm
	}

	return &ConcurrentWasmVMContext{
		concurrentWasmVMs: wasmVMs,
	}, nil
}

// Next
func (c *ConcurrentWasmVMContext) AssignNext(ctx sdk.Context) sdk.Context {
	_, ok := ctx.Context().Value(contextKeyAllocatedVMIndex).(int)

	// if vm index is never assigned upon arriving this method,
	// this context is coming from an unusual route
	// try & force allocate
	if ok {
		return ctx
	}

	ci := atomic.AddUint64(&c.i, 1)
	fmt.Println("----- assigned wasmvm index for query", ci)
	return ctx.WithContext(context.WithValue(ctx.Context(), contextKeyAllocatedVMIndex, ci%uint64(c.config.NumParallelism)))
}

func (c *ConcurrentWasmVMContext) Get(idx uint64) types.WasmerEngine {
	return c.concurrentWasmVMs[idx]
}

func setExecutionType(ctx sdk.Context, executionType ExecutionType) sdk.Context {
	fmt.Println("-----  setting execution type", executionType)
	return ctx.WithContext(context.WithValue(ctx.Context(), contextKeyExecutionType, executionType))
}

func getExecutionType(ctx sdk.Context) ExecutionType {
	execType, ok := ctx.Context().Value(contextKeyExecutionType).(ExecutionType)

	if !ok {
		return ExecutionTypeQuery
	}

	switch execType {
	case ExecutionTypeExecution:
		return ExecutionTypeExecution
	case ExecutionTypeQuery:
		return ExecutionTypeQuery
	default:
		// unknown execType, fallback to query
		return ExecutionTypeQuery
	}
}

// extend keeper
func (k Keeper) getWasmVM(ctx sdk.Context) types.WasmerEngine {
	if k.concurrentWasmVMContext == nil {
		return k.wasmVM
	}

	execType := getExecutionType(ctx)

	switch execType {
	case ExecutionTypeExecution:
		return k.wasmVM
	default:
		assignedVM, ok := ctx.Context().Value(contextKeyAllocatedVMIndex).(uint64)
		if !ok {
			panic("cannot arrive here")
		}

		return k.concurrentWasmVMContext.Get(assignedVM)
	}
}
