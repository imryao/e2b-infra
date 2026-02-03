package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// transitionError represents an error stored in the transition key
type transitionError struct {
	Error string `json:"error"`
}

// Add stores a sandbox in Redis atomically with its team index entry.
func (s *Storage) Add(ctx context.Context, sbx sandbox.Sandbox) error {
	data, err := json.Marshal(sbx)
	if err != nil {
		return fmt.Errorf("failed to marshal sandbox: %w", err)
	}

	key := getSandboxKey(sbx.TeamID.String(), sbx.SandboxID)
	teamKey := getTeamIndexKey(sbx.TeamID.String())

	// Execute Lua script for atomic SET + SADD
	err = addSandboxScript.Run(ctx, s.redisClient, []string{key, teamKey}, data, sbx.SandboxID).Err()
	if err != nil {
		return fmt.Errorf("failed to store sandbox in Redis: %w", err)
	}

	return nil
}

// Get retrieves a sandbox from Redis
func (s *Storage) Get(ctx context.Context, teamID uuid.UUID, sandboxID string) (sandbox.Sandbox, error) {
	key := getSandboxKey(teamID.String(), sandboxID)
	data, err := s.redisClient.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return sandbox.Sandbox{}, &sandbox.NotFoundError{SandboxID: sandboxID}
	}
	if err != nil {
		return sandbox.Sandbox{}, fmt.Errorf("failed to get sandbox from Redis: %w", err)
	}

	var sbx sandbox.Sandbox
	err = json.Unmarshal(data, &sbx)
	if err != nil {
		return sandbox.Sandbox{}, fmt.Errorf("failed to unmarshal sandbox: %w", err)
	}

	return sbx, nil
}

// Remove deletes a sandbox from Redis atomically with its team index entry.
func (s *Storage) Remove(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	key := getSandboxKey(teamID.String(), sandboxID)
	teamKey := getTeamIndexKey(teamID.String())

	lock, err := s.lockService.Obtain(ctx, redis_utils.GetLockKey(key), lockTimeout, s.lockOption)
	if err != nil {
		return fmt.Errorf("failed to obtain lock: %w", err)
	}

	defer func() {
		err := lock.Release(context.WithoutCancel(ctx))
		if err != nil {
			logger.L().Error(ctx, "Failed to release lock", zap.Error(err))
		}
	}()

	// Execute Lua script for atomic DEL + SREM
	err = removeSandboxScript.Run(ctx, s.redisClient, []string{key, teamKey}, sandboxID).Err()
	if err != nil {
		return fmt.Errorf("failed to remove sandbox from Redis: %w", err)
	}

	return nil
}

// TeamItems retrieves sandboxes for a specific team, filtered by states and options
func (s *Storage) TeamItems(ctx context.Context, teamID uuid.UUID, states []sandbox.State) ([]sandbox.Sandbox, error) {
	// Get sandbox IDs from team index
	teamKey := getTeamIndexKey(teamID.String())
	sandboxIDs, err := s.redisClient.SMembers(ctx, teamKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get sandbox IDs from team index: %w", err)
	}

	if len(sandboxIDs) == 0 {
		return []sandbox.Sandbox{}, nil
	}

	// Build keys and batch fetch with MGET
	team := teamID.String()
	keys := utils.Map(sandboxIDs, func(id string) string {
		return getSandboxKey(team, id)
	})

	results, err := s.redisClient.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get sandboxes from Redis: %w", err)
	}

	// Deserialize and filter
	var sandboxes []sandbox.Sandbox
	for _, rawResult := range results {
		if rawResult == nil {
			continue // Stale index entry - sandbox was deleted
		}

		var sbx sandbox.Sandbox
		result, ok := rawResult.(string)
		if !ok {
			logger.L().Error(ctx, "Invalid sandbox data type in Redis")

			continue
		}

		if err := json.Unmarshal([]byte(result), &sbx); err != nil {
			logger.L().Error(ctx, "Failed to unmarshal sandbox", zap.Error(err))

			continue
		}

		// Filter by state if states are specified
		if len(states) > 0 && !slices.Contains(states, sbx.State) {
			continue
		}

		sandboxes = append(sandboxes, sbx)
	}

	return sandboxes, nil
}

// Update modifies a sandbox atomically
func (s *Storage) Update(ctx context.Context, teamID uuid.UUID, sandboxID string, updateFunc func(sandbox.Sandbox) (sandbox.Sandbox, error)) (sandbox.Sandbox, error) {
	key := getSandboxKey(teamID.String(), sandboxID)

	lock, err := s.lockService.Obtain(ctx, redis_utils.GetLockKey(key), lockTimeout, s.lockOption)
	if err != nil {
		return sandbox.Sandbox{}, fmt.Errorf("failed to obtain lock: %w", err)
	}

	defer func() {
		err := lock.Release(context.WithoutCancel(ctx))
		if err != nil {
			logger.L().Error(ctx, "Failed to release lock", zap.Error(err))
		}
	}()

	// Get current value
	data, err := s.redisClient.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return sandbox.Sandbox{}, &sandbox.NotFoundError{SandboxID: sandboxID}
	}
	if err != nil {
		return sandbox.Sandbox{}, err
	}

	var sbx sandbox.Sandbox
	err = json.Unmarshal(data, &sbx)
	if err != nil {
		return sandbox.Sandbox{}, err
	}

	// Apply update
	updatedSbx, err := updateFunc(sbx)
	if err != nil {
		return sandbox.Sandbox{}, fmt.Errorf("failed to update sandbox: %w", err)
	}

	// Serialize updated sandbox
	newData, err := json.Marshal(updatedSbx)
	if err != nil {
		return sandbox.Sandbox{}, err
	}

	// Execute transaction
	err = s.redisClient.Set(ctx, key, newData, redis.KeepTTL).Err()
	if err != nil {
		return sandbox.Sandbox{}, fmt.Errorf("failed to store sandbox in Redis: %w", err)
	}

	return updatedSbx, nil
}

func (s *Storage) AllItems(_ context.Context, _ []sandbox.State, _ ...sandbox.ItemsOption) ([]sandbox.Sandbox, error) {
	// TODO: Implement later (ENG-3451)
	return nil, nil
}

// StartRemoving initiates the removal process for a sandbox using atomic Lua scripts.
//
// The function handles concurrent requests safely:
//  1. Acquires a distributed lock on the sandbox
//  2. Checks if there's an ongoing transition via the transition key:
//     - If the same target state is in progress, waits for completion and returns the result
//     - If a different state is in progress, waits for it to complete, then retries
//  3. Validates the state transition is allowed
//  4. Atomically updates the sandbox state and sets the transition key
//  5. Returns a callback that the caller must invoke to signal completion
//
// The callback is critical: it either deletes the transition key on success
// or sets an error value with short TTL to notify waiters of the failure.
func (s *Storage) StartRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, stateAction sandbox.StateAction) (alreadyDone bool, callback func(context.Context, error), err error) {
	// Determine target state from stateAction
	newState := sandbox.StateKilling
	if stateAction == sandbox.StateActionPause {
		newState = sandbox.StatePausing
	}

	key := getSandboxKey(teamID.String(), sandboxID)
	transitionKey := getTransitionKey(teamID.String(), sandboxID)

	// Acquire distributed lock
	lock, err := s.lockService.Obtain(ctx, redis_utils.GetLockKey(key), lockTimeout, s.lockOption)
	if err != nil {
		return false, nil, fmt.Errorf("failed to obtain lock: %w", err)
	}

	// Ensure lock is released once
	releaseFunc := sync.OnceValue(func() error {
		return lock.Release(context.WithoutCancel(ctx))
	})

	defer func() {
		releaseErr := releaseFunc()
		if releaseErr != nil {
			logger.L().Error(ctx, "Failed to release lock", zap.Error(releaseErr))
		}
	}()

	// Get current sandbox state first
	data, err := s.redisClient.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return false, nil, &sandbox.NotFoundError{SandboxID: sandboxID}
	}
	if err != nil {
		return false, nil, fmt.Errorf("failed to get sandbox from Redis: %w", err)
	}

	var sbx sandbox.Sandbox
	if err = json.Unmarshal(data, &sbx); err != nil {
		return false, nil, fmt.Errorf("failed to unmarshal sandbox: %w", err)
	}

	// Check if there's an existing transition
	existingTransition, err := s.redisClient.Get(ctx, transitionKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, nil, fmt.Errorf("failed to check transition key: %w", err)
	}

	if existingTransition != "" {
		releaseErr := releaseFunc()
		if releaseErr != nil {
			logger.L().Warn(ctx, "Failed to release lock before waiting", zap.Error(releaseErr))
		}

		return s.handleExistingTransition(ctx, teamID, sbx, stateAction, newState)
	}

	// Check if already in target state
	if sbx.State == newState {
		logger.L().Debug(ctx, "Already in the same state", logger.WithSandboxID(sandboxID), zap.String("state", string(newState)))

		return true, func(context.Context, error) {}, nil
	}

	// Validate state transition is allowed
	if !sandbox.AllowedTransitions[sbx.State][newState] {
		return false, nil, fmt.Errorf("invalid state transition from %s to %s", sbx.State, newState)
	}

	// Update sandbox state
	sbx.State = newState
	// Mark as expired if not already
	if !sbx.IsExpired() {
		sbx.EndTime = time.Now()
	}

	newData, err := json.Marshal(sbx)
	if err != nil {
		return false, nil, fmt.Errorf("failed to marshal sandbox: %w", err)
	}

	// Use atomic Lua script to update sandbox and set transition key
	ttlSeconds := int(transitionKeyTTL.Seconds())
	err = startTransitionScript.Run(ctx, s.redisClient, []string{key, transitionKey}, newData, ttlSeconds).Err()
	if err != nil {
		return false, nil, fmt.Errorf("failed to update sandbox state: %w", err)
	}

	logger.L().Debug(ctx, "Started state transition", logger.WithSandboxID(sandboxID), zap.String("state", string(newState)))

	return false, s.createCallback(sandboxID, transitionKey, newState), nil
}

// createCallback returns a callback function for completing a transition.
// On success, the callback deletes the transition key.
// On error, the callback sets an error value with short TTL for waiters to see.
func (s *Storage) createCallback(sandboxID, transitionKey string, state sandbox.State) func(context.Context, error) {
	return func(cbCtx context.Context, err error) {
		logger.L().Debug(cbCtx, "Transition complete", logger.WithSandboxID(sandboxID), zap.String("state", string(state)), zap.Error(err))

		lock, lockErr := s.lockService.Obtain(cbCtx, redis_utils.GetLockKey(transitionKey), lockTimeout, s.lockOption)
		if lockErr != nil {
			logger.L().Error(cbCtx, "Failed to obtain lock for transition callback", logger.WithSandboxID(sandboxID), zap.Error(lockErr))

			return
		}
		defer func() {
			lockErr = lock.Release(context.WithoutCancel(cbCtx))
			if lockErr != nil {
				logger.L().Error(cbCtx, "Failed to release lock for transition callback", logger.WithSandboxID(sandboxID), zap.Error(lockErr))
			}
		}()

		if err == nil {
			// Success: delete the key
			delErr := s.redisClient.Del(cbCtx, transitionKey).Err()
			if delErr != nil {
				logger.L().Warn(cbCtx, "Failed to delete transition key", logger.WithSandboxID(sandboxID), zap.Error(delErr))
			}

			return
		}

		// Error: set error value with short TTL for waiters
		errorData, marshalErr := json.Marshal(transitionError{Error: err.Error()})
		if marshalErr != nil {
			logger.L().Error(cbCtx, "Failed to marshal transition error", logger.WithSandboxID(sandboxID), zap.Error(marshalErr))

			return
		}

		setErr := s.redisClient.Set(cbCtx, transitionKey, errorData, errorTransitionTTL).Err()
		if setErr != nil {
			logger.L().Warn(cbCtx, "Failed to set error transition", logger.WithSandboxID(sandboxID), zap.Error(setErr))
		}
	}
}

// WaitForStateChange waits for a sandbox state transition to complete.
// This is a public API that waits for any ongoing transition.
func (s *Storage) WaitForStateChange(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	return s.waitForTransition(ctx, teamID, sandboxID)
}

// waitForTransition waits for a specific transition to complete.
// Returns nil on success, or the error if the transition failed.
func (s *Storage) waitForTransition(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	transitionKey := getTransitionKey(teamID.String(), sandboxID)

	for {
		value, err := s.redisClient.Get(ctx, transitionKey).Result()
		if errors.Is(err, redis.Nil) {
			// Key gone = transition done successfully
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to check transition key: %w", err)
		}

		// Check if it's an error value (JSON with "error" field)
		var transErr transitionError
		if err = json.Unmarshal([]byte(value), &transErr); err != nil {
			return fmt.Errorf("failed to unmarshal transition error: %w", err)
		}

		if transErr.Error != "" {
			return fmt.Errorf("transition failed: %s", transErr.Error)
		}

		// Wait before next poll
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryInterval):
			// Continue polling
		}
	}
}

func (s *Storage) handleExistingTransition(ctx context.Context, teamID uuid.UUID, sbx sandbox.Sandbox, stateAction sandbox.StateAction, newState sandbox.State) (bool, func(context.Context, error), error) {
	if sbx.State == newState {
		// Same target state - wait for completion and return alreadyDone=true
		logger.L().Debug(ctx, "State transition already in progress to the same state, waiting",
			logger.WithSandboxID(sbx.SandboxID),
			zap.String("state", string(newState)))

		waitErr := s.waitForTransition(ctx, teamID, sbx.SandboxID)
		if waitErr != nil {
			return false, nil, fmt.Errorf("failed waiting for transition: %w", waitErr)
		}

		return true, func(context.Context, error) {}, nil
	}

	// Different state - validate transition and wait
	if !sandbox.AllowedTransitions[sbx.State][newState] {
		return false, nil, fmt.Errorf("invalid state transition, already in transition from %s", sbx.State)
	}

	waitErr := s.waitForTransition(ctx, teamID, sbx.SandboxID)
	if waitErr != nil {
		return false, nil, fmt.Errorf("failed waiting for transition: %w", waitErr)
	}

	// Retry with new state after transition completes
	return s.StartRemoving(ctx, teamID, sbx.SandboxID, stateAction)
}
