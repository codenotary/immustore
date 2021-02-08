/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package database

import "github.com/codenotary/immudb/embedded/store"

//DbOptions database instance options
type DbOptions struct {
	//	dbDir             string
	dbName            string
	dbRootPath        string
	corruptionChecker bool
	storeOpts         *store.Options
}

// DefaultOption Initialise Db Optionts to default values
func DefaultOption() *DbOptions {
	return &DbOptions{
		//		dbDir:             "immudb",
		dbName:            "db_name",
		dbRootPath:        "./data",
		corruptionChecker: true,
		storeOpts:         store.DefaultOptions(),
	}
}

// WithDbName sets dbName, which is also db instance directory
func (o *DbOptions) WithDbName(dbName string) *DbOptions {
	o.dbName = dbName
	return o
}

// GetDbName Returns Database name which is also db instance directory
func (o *DbOptions) GetDbName() string {
	return o.dbName
}

// WithDbRootPath sets the directory in which this database will reside
func (o *DbOptions) WithDbRootPath(Path string) *DbOptions {
	o.dbRootPath = Path
	return o
}

// GetDbRootPath returns the directory in which this database resides
func (o *DbOptions) GetDbRootPath() string {
	return o.dbRootPath
}

// WithCorruptionChecker sets if corruption checker should start for this database instance
func (o *DbOptions) WithCorruptionChecker(cc bool) *DbOptions {
	o.corruptionChecker = cc
	return o
}

// GetCorruptionChecker returns if corruption checker should start for this database instance
func (o *DbOptions) GetCorruptionChecker() bool {
	return o.corruptionChecker
}

// WithStoreOptions sets backing store options
func (o *DbOptions) WithStoreOptions(storeOpts *store.Options) *DbOptions {
	o.storeOpts = storeOpts
	return o
}

// GetStoreOptions returns backing store options
func (o *DbOptions) GetStoreOptions() *store.Options {
	return o.storeOpts
}
