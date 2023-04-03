// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dsess

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
)

// SessionStateAdapter is an adapter for env.RepoStateReader in SQL contexts, getting information about the repo state
// from the session.
type SessionStateAdapter struct {
	session  *DoltSession
	dbName   string
	remotes  map[string]env.Remote
	backups  map[string]env.Remote
	branches map[string]env.BranchConfig
}

func (s SessionStateAdapter) UpdateStagedRoot(ctx context.Context, newRoot *doltdb.RootValue) error {
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		return fmt.Errorf("non-sql context passed to SessionStateAdapter")
	}
	roots, _ := s.session.GetRoots(sqlCtx, s.dbName)
	roots.Staged = newRoot
	return s.session.SetRoots(ctx.(*sql.Context), s.dbName, roots)
}

func (s SessionStateAdapter) UpdateWorkingRoot(ctx context.Context, newRoot *doltdb.RootValue) error {
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		return fmt.Errorf("non-sql context passed to SessionStateAdapter")
	}
	roots, _ := s.session.GetRoots(sqlCtx, s.dbName)
	roots.Working = newRoot
	return s.session.SetRoots(ctx.(*sql.Context), s.dbName, roots)
}

func (s SessionStateAdapter) SetCWBHeadRef(_ context.Context, _ ref.MarshalableRef) error {
	return fmt.Errorf("Cannot set cwb head ref with a SessionStateAdapter")
}

func (s SessionStateAdapter) AbortMerge(_ context.Context) error {
	return fmt.Errorf("Cannot abort merge with a SessionStateAdapter")
}

func (s SessionStateAdapter) ClearMerge(_ context.Context) error {
	return nil
}

func (s SessionStateAdapter) StartMerge(_ context.Context, _ *doltdb.Commit) error {
	return fmt.Errorf("Cannot start merge with a SessionStateAdapter")
}

var _ env.RepoStateReader = SessionStateAdapter{}
var _ env.RepoStateWriter = SessionStateAdapter{}
var _ env.RootsProvider = SessionStateAdapter{}

func NewSessionStateAdapter(session *DoltSession, dbName string, remotes map[string]env.Remote, branches map[string]env.BranchConfig, backups map[string]env.Remote) SessionStateAdapter {
	if branches == nil {
		branches = make(map[string]env.BranchConfig)
	}
	return SessionStateAdapter{session: session, dbName: dbName, remotes: remotes, branches: branches, backups: backups}
}

func (s SessionStateAdapter) GetRoots(ctx context.Context) (doltdb.Roots, error) {
	sqlCtx := sql.NewContext(ctx)
	state, _, err := s.session.LookupDbState(sqlCtx, s.dbName)
	if err != nil {
		return doltdb.Roots{}, err
	}

	return state.GetRoots(), nil
}

func (s SessionStateAdapter) CWBHeadRef() ref.DoltRef {
	workingSet, err := s.session.WorkingSet(sql.NewContext(context.Background()), s.dbName)
	if err != nil {
		// TODO: fix this interface
		panic(err)
	}

	headRef, err := workingSet.Ref().ToHeadRef()
	// TODO: fix this interface
	if err != nil {
		panic(err)
	}
	return headRef
}

func (s SessionStateAdapter) CWBHeadSpec() *doltdb.CommitSpec {
	// TODO: get rid of this
	ref := s.CWBHeadRef()
	spec, err := doltdb.NewCommitSpec(ref.GetPath())
	if err != nil {
		panic(err)
	}
	return spec
}

func (s SessionStateAdapter) IsMergeActive(ctx context.Context) (bool, error) {
	workingSet, err := s.session.WorkingSet(sql.NewContext(context.Background()), s.dbName)
	if err != nil {
		return false, err
	}

	return workingSet.MergeActive(), nil
}

func (s SessionStateAdapter) GetMergeCommit(ctx context.Context) (*doltdb.Commit, error) {
	workingSet, err := s.session.WorkingSet(sql.NewContext(context.Background()), s.dbName)
	if err != nil {
		return nil, err
	}
	return workingSet.MergeState().Commit(), nil
}

func (s SessionStateAdapter) GetPreMergeWorking(ctx context.Context) (*doltdb.RootValue, error) {
	workingSet, err := s.session.WorkingSet(sql.NewContext(context.Background()), s.dbName)
	if err != nil {
		return nil, err
	}

	return workingSet.MergeState().PreMergeWorkingRoot(), nil
}

func (s SessionStateAdapter) GetRemotes() (map[string]env.Remote, error) {
	return s.remotes, nil
}

func (s SessionStateAdapter) GetBackups() (map[string]env.Remote, error) {
	return s.backups, nil
}

func (s SessionStateAdapter) GetBranches() (map[string]env.BranchConfig, error) {
	return s.branches, nil
}

func (s SessionStateAdapter) UpdateBranch(name string, new env.BranchConfig) error {
	s.branches[name] = new

	fs, err := s.session.Provider().FileSystemForDatabase(s.dbName)
	if err != nil {
		return err
	}

	repoState, err := env.LoadRepoState(fs)
	if err != nil {
		return err
	}
	repoState.Branches[name] = new

	return repoState.Save(fs)
}

func (s SessionStateAdapter) AddRemote(remote env.Remote) error {
	if _, ok := s.remotes[remote.Name]; ok {
		return env.ErrRemoteAlreadyExists
	}

	if strings.IndexAny(remote.Name, " \t\n\r./\\!@#$%^&*(){}[],.<>'\"?=+|") != -1 {
		return env.ErrInvalidBackupName
	}

	fs, err := s.session.Provider().FileSystemForDatabase(s.dbName)
	if err != nil {
		return err
	}

	repoState, err := env.LoadRepoState(fs)
	if err != nil {
		return err
	}

	// can have multiple remotes with the same address, but no conflicting backups
	if rem, found := env.CheckRemoteAddressConflict(remote.Url, nil, repoState.Backups); found {
		return fmt.Errorf("%w: '%s' -> %s", env.ErrRemoteAddressConflict, rem.Name, rem.Url)
	}

	s.remotes[remote.Name] = remote
	repoState.AddRemote(remote)
	return repoState.Save(fs)
}

func (s SessionStateAdapter) AddBackup(backup env.Remote) error {
	if _, ok := s.backups[backup.Name]; ok {
		return env.ErrBackupAlreadyExists
	}

	if strings.IndexAny(backup.Name, " \t\n\r./\\!@#$%^&*(){}[],.<>'\"?=+|") != -1 {
		return env.ErrInvalidBackupName
	}

	fs, err := s.session.Provider().FileSystemForDatabase(s.dbName)
	if err != nil {
		return err
	}

	repoState, err := env.LoadRepoState(fs)
	if err != nil {
		return err
	}

	// no conflicting remote or backup addresses
	if bac, found := env.CheckRemoteAddressConflict(backup.Url, repoState.Remotes, repoState.Backups); found {
		return fmt.Errorf("%w: '%s' -> %s", env.ErrRemoteAddressConflict, bac.Name, bac.Url)
	}

	s.backups[backup.Name] = backup
	repoState.AddBackup(backup)
	return repoState.Save(fs)
}

func (s SessionStateAdapter) RemoveRemote(_ context.Context, name string) error {
	remote, ok := s.remotes[name]
	if !ok {
		return env.ErrRemoteNotFound
	}
	delete(s.remotes, remote.Name)

	fs, err := s.session.Provider().FileSystemForDatabase(s.dbName)
	if err != nil {
		return err
	}

	repoState, err := env.LoadRepoState(fs)
	if err != nil {
		return err
	}

	remote, ok = repoState.Remotes[name]
	if !ok {
		// sanity check
		return env.ErrRemoteNotFound
	}
	delete(repoState.Remotes, name)
	return repoState.Save(fs)
}

func (s SessionStateAdapter) RemoveBackup(_ context.Context, name string) error {
	backup, ok := s.backups[name]
	if !ok {
		return env.ErrBackupNotFound
	}
	delete(s.backups, backup.Name)

	fs, err := s.session.Provider().FileSystemForDatabase(s.dbName)
	if err != nil {
		return err
	}

	repoState, err := env.LoadRepoState(fs)
	if err != nil {
		return err
	}

	backup, ok = repoState.Backups[name]
	if !ok {
		// sanity check
		return env.ErrBackupNotFound
	}
	delete(repoState.Backups, name)
	return repoState.Save(fs)
}

func (s SessionStateAdapter) TempTableFilesDir() (string, error) {
	state, _, err := s.session.LookupDbState(sql.NewContext(context.Background()), s.dbName)
	if err != nil {
		return "", err
	}

	return state.tmpFileDir, nil
}
