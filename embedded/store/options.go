/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package store

import (
	"fmt"
	"os"
	"time"

	"github.com/codenotary/immudb/embedded/ahtree"
	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/cache"
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/pkg/helpers/semaphore"
)

const DefaultMaxActiveTransactions = 1000
const DefaultMVCCReadSetLimit = 100_000
const DefaultMaxConcurrency = 30
const DefaultMaxIOConcurrency = 1
const DefaultMaxTxEntries = 1 << 10 // 1024
const DefaultMaxKeyLen = 1024
const DefaultMaxValueLen = 4096 // 4Kb
const DefaultSyncFrequency = 20 * time.Millisecond
const DefaultFileMode = os.FileMode(0755)
const DefaultFileSize = multiapp.DefaultFileSize
const DefaultCompressionFormat = appendable.DefaultCompressionFormat
const DefaultCompressionLevel = appendable.DefaultCompressionLevel
const DefaultEmbeddedValues = false
const DefaultPreallocFiles = false
const DefaultTxLogCacheSize = 1000
const DefaultVLogCacheSize = 0
const DefaultMaxWaitees = 1000
const DefaultVLogMaxOpenedFiles = 10
const DefaultTxLogMaxOpenedFiles = 10
const DefaultCommitLogMaxOpenedFiles = 10
const DefaultWriteTxHeaderVersion = MaxTxHeaderVersion
const DefaultWriteBufferSize = 1 << 22 //4Mb
const DefaultIndexingMaxBulkSize = 1
const DefaultIndexingGlobalMaxBufferedDataSize = 1 << 30
const DefaultBulkPreparationTimeout = DefaultSyncFrequency
const DefaultTruncationFrequency = 24 * time.Hour
const MinimumRetentionPeriod = 24 * time.Hour
const MinimumTruncationFrequency = 1 * time.Hour

const MaxFileSize = (1 << 31) - 1 // 2Gb

type AppFactoryFunc func(
	rootPath string,
	subPath string,
	opts *multiapp.Options,
) (appendable.Appendable, error)

type AppRemoveFunc func(rootPath, subPath string) error

type IndexCacheFactoryFunc func() *cache.Cache

type IndexMemSemaphoreFactoryFunc func() *semaphore.Semaphore

type TimeFunc func() time.Time

type FlushFunc func() error

type Options struct {
	ReadOnly bool

	// Fsync during commit process
	Synced bool

	// Fsync frequency during commit process
	SyncFrequency time.Duration

	// Size of the in-memory buffer for write operations
	WriteBufferSize int

	FileMode os.FileMode

	logger logger.Logger

	appFactory AppFactoryFunc

	appRemove AppRemoveFunc

	CompactionDisabled bool

	// Maximum number of pre-committed transactions
	MaxActiveTransactions int

	// Limit the number of read entries per transaction
	MVCCReadSetLimit int

	// Maximum number of simultaneous commits prepared for write
	MaxConcurrency int

	// Maximum number of simultaneous IO writes
	MaxIOConcurrency int

	// Size of the cache for transaction logs
	TxLogCacheSize int

	// Maximum number of simultaneous value files opened
	VLogMaxOpenedFiles int

	// Size of the cache for value logs
	VLogCacheSize int

	// Maximum number of simultaneous transaction log files opened
	TxLogMaxOpenedFiles int

	// Maximum number of simultaneous commit log files opened
	CommitLogMaxOpenedFiles int

	// Version of transaction header to use (limits available features)
	WriteTxHeaderVersion int

	// Maximum number of go-routines waiting for specific transactions to be in a committed or indexed state
	MaxWaitees int

	TimeFunc TimeFunc

	UseExternalCommitAllowance bool

	MultiIndexing bool

	// options below are only set during initialization and stored as metadata
	MaxTxEntries      int
	MaxKeyLen         int
	MaxValueLen       int
	FileSize          int
	CompressionFormat int
	CompressionLevel  int
	EmbeddedValues    bool
	PreallocFiles     bool
	// Discard processing of transactions that were precommitted before opening
	DiscardPrecommittedTransactions bool

	// options below affect indexing
	IndexOpts *IndexOptions

	// options below affect appendable hash tree
	AHTOpts *AHTOptions
}

type IndexOptions struct {
	// Global limit for amount of data that can be buffered from all indexes
	MaxGlobalBufferedDataSize int

	// MaxBufferedDataSize
	MaxBufferedDataSize int

	// Size (in bytes) of the Btree node cache
	CacheSize int

	// Number of new index entries between disk flushes
	FlushThld int

	// Number of new index entries between disk flushes with file sync
	SyncThld int

	// Size of the in-memory flush buffer (in bytes)
	FlushBufferSize int

	// Percentage of node files cleaned up during each flush
	CleanupPercentage float32

	// Maximum number of active btree snapshots
	MaxActiveSnapshots int

	// Max size of a single Btree node in bytes
	MaxNodeSize int

	// Time between the most recent DB snapshot is automatically renewed
	RenewSnapRootAfter time.Duration

	// Minimum number of updates entries in the btree to allow for full compaction
	CompactionThld int

	// Additional delay added during indexing when full compaction is in progress
	DelayDuringCompaction time.Duration

	// Maximum number of simultaneously opened nodes files
	NodesLogMaxOpenedFiles int

	// Maximum number of simultaneously opened node history files
	HistoryLogMaxOpenedFiles int

	// Maximum number of simultaneously opened commit log files
	CommitLogMaxOpenedFiles int

	// Maximum number of transactions indexed together
	MaxBulkSize int

	// Maximum time waiting for more transactions to be committed and included into the same bulk
	BulkPreparationTimeout time.Duration
}

type AHTOptions struct {
	// Number of new leaves in the tree between synchronous flush to disk
	SyncThld int

	// Size of the in-memory write buffer
	WriteBufferSize int
}

func DefaultOptions() *Options {
	return &Options{
		ReadOnly:        false,
		WriteBufferSize: DefaultWriteBufferSize,
		Synced:          true,
		SyncFrequency:   DefaultSyncFrequency,
		FileMode:        DefaultFileMode,
		logger:          logger.NewSimpleLogger("immudb ", os.Stderr),

		MaxActiveTransactions: DefaultMaxActiveTransactions,
		MVCCReadSetLimit:      DefaultMVCCReadSetLimit,

		MaxConcurrency:   DefaultMaxConcurrency,
		MaxIOConcurrency: DefaultMaxIOConcurrency,

		TxLogCacheSize: DefaultTxLogCacheSize,
		VLogCacheSize:  DefaultVLogCacheSize,

		VLogMaxOpenedFiles:      DefaultVLogMaxOpenedFiles,
		TxLogMaxOpenedFiles:     DefaultTxLogMaxOpenedFiles,
		CommitLogMaxOpenedFiles: DefaultCommitLogMaxOpenedFiles,

		MaxWaitees: DefaultMaxWaitees,

		TimeFunc: func() time.Time {
			return time.Now()
		},

		WriteTxHeaderVersion: DefaultWriteTxHeaderVersion,

		// options below are only set during initialization and stored as metadata
		MaxTxEntries:                    DefaultMaxTxEntries,
		MaxKeyLen:                       DefaultMaxKeyLen,
		MaxValueLen:                     DefaultMaxValueLen,
		FileSize:                        DefaultFileSize,
		CompressionFormat:               DefaultCompressionFormat,
		CompressionLevel:                DefaultCompressionLevel,
		EmbeddedValues:                  DefaultEmbeddedValues,
		PreallocFiles:                   DefaultPreallocFiles,
		DiscardPrecommittedTransactions: false,
		IndexOpts:                       DefaultIndexOptions(),
		AHTOpts:                         DefaultAHTOptions(),
	}
}

func DefaultIndexOptions() *IndexOptions {
	return &IndexOptions{
		MaxGlobalBufferedDataSize: DefaultIndexingGlobalMaxBufferedDataSize,
		MaxBufferedDataSize:       tbtree.DefaultMaxBufferedDataSize,
		CacheSize:                 tbtree.DefaultCacheSize,
		FlushThld:                 tbtree.DefaultFlushThld,
		SyncThld:                  tbtree.DefaultSyncThld,
		FlushBufferSize:           tbtree.DefaultFlushBufferSize,
		CleanupPercentage:         tbtree.DefaultCleanUpPercentage,
		MaxActiveSnapshots:        tbtree.DefaultMaxActiveSnapshots,
		MaxNodeSize:               tbtree.DefaultMaxNodeSize,
		RenewSnapRootAfter:        tbtree.DefaultRenewSnapRootAfter,
		CompactionThld:            tbtree.DefaultCompactionThld,
		DelayDuringCompaction:     0,
		NodesLogMaxOpenedFiles:    tbtree.DefaultNodesLogMaxOpenedFiles,
		HistoryLogMaxOpenedFiles:  tbtree.DefaultHistoryLogMaxOpenedFiles,
		CommitLogMaxOpenedFiles:   tbtree.DefaultCommitLogMaxOpenedFiles,

		MaxBulkSize:            DefaultIndexingMaxBulkSize,
		BulkPreparationTimeout: DefaultBulkPreparationTimeout,
	}
}

func DefaultAHTOptions() *AHTOptions {
	return &AHTOptions{
		SyncThld:        ahtree.DefaultSyncThld,
		WriteBufferSize: ahtree.DefaultWriteBufferSize,
	}
}

func (opts *Options) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", ErrInvalidOptions)
	}

	if opts.WriteBufferSize <= 0 {
		return fmt.Errorf("%w: invalid WriteBufferSize", ErrInvalidOptions)
	}
	if opts.SyncFrequency < 0 {
		return fmt.Errorf("%w: invalid SyncFrequency", ErrInvalidOptions)
	}

	if opts.MaxActiveTransactions <= 0 {
		return fmt.Errorf("%w: invalid MaxActiveTransactions", ErrInvalidOptions)
	}

	if opts.MVCCReadSetLimit <= 0 {
		return fmt.Errorf("%w: invalid MVCCReadSetLimit", ErrInvalidOptions)
	}

	if opts.MaxConcurrency <= 0 {
		return fmt.Errorf("%w: invalid MaxConcurrency", ErrInvalidOptions)
	}

	if opts.MaxIOConcurrency <= 0 ||
		opts.MaxIOConcurrency > MaxParallelIO ||
		(opts.MaxIOConcurrency > 1 && opts.EmbeddedValues) {
		return fmt.Errorf("%w: invalid MaxIOConcurrency", ErrInvalidOptions)
	}

	if opts.VLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid VLogMaxOpenedFiles", ErrInvalidOptions)
	}
	if opts.TxLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid TxLogMaxOpenedFiles", ErrInvalidOptions)
	}
	if opts.CommitLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid CommitLogMaxOpenedFiles", ErrInvalidOptions)
	}

	if opts.TxLogCacheSize <= 0 {
		return fmt.Errorf("%w: invalid TxLogCacheSize", ErrInvalidOptions)
	}

	if opts.VLogCacheSize < 0 {
		return fmt.Errorf("%w: invalid VLogCacheSize", ErrInvalidOptions)
	}

	if opts.MaxWaitees < 0 {
		return fmt.Errorf("%w: invalid MaxWaitees", ErrInvalidOptions)
	}

	if opts.TimeFunc == nil {
		return fmt.Errorf("%w: invalid TimeFunc", ErrInvalidOptions)
	}

	if opts.WriteTxHeaderVersion < 0 {
		return fmt.Errorf("%w: invalid WriteTxHeaderVersion", ErrInvalidOptions)
	}
	if opts.WriteTxHeaderVersion > MaxTxHeaderVersion {
		return fmt.Errorf("%w: invalid WriteTxHeaderVersion", ErrInvalidOptions)
	}

	// options below are only set during initialization and stored as metadata
	if opts.MaxTxEntries <= 0 {
		return fmt.Errorf("%w: invalid MaxTxEntries", ErrInvalidOptions)
	}
	if opts.MaxKeyLen <= 0 || opts.MaxKeyLen > MaxKeyLen {
		return fmt.Errorf("%w: invalid MaxKeyLen", ErrInvalidOptions)
	}
	if opts.MaxValueLen <= 0 {
		return fmt.Errorf("%w: invalid MaxValueLen", ErrInvalidOptions)
	}
	if opts.FileSize <= 0 || opts.FileSize >= MaxFileSize {
		return fmt.Errorf("%w: invalid FileSize", ErrInvalidOptions)
	}
	if opts.logger == nil {
		return fmt.Errorf("%w: invalid log", ErrInvalidOptions)
	}

	err := opts.IndexOpts.Validate()
	if err != nil {
		return err
	}

	return opts.AHTOpts.Validate()
}

func (opts *IndexOptions) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil index options ", ErrInvalidOptions)
	}
	if opts.MaxBufferedDataSize <= 0 {
		return fmt.Errorf("%w: invalid index option MaxBufferedDataSize", ErrInvalidOptions)
	}
	if opts.MaxGlobalBufferedDataSize <= 0 {
		return fmt.Errorf("%w: invalid index option MaxGlobalBufferedDataSize", ErrInvalidOptions)
	}
	if opts.MaxBufferedDataSize > opts.MaxGlobalBufferedDataSize {
		return fmt.Errorf("%w: invalid index option MaxBufferedDataSize > MaxGlobalBufferedDataSize", ErrInvalidOptions)
	}
	if opts.CacheSize <= 0 {
		return fmt.Errorf("%w: invalid index option CacheSize", ErrInvalidOptions)
	}
	if opts.FlushThld <= 0 {
		return fmt.Errorf("%w: invalid index option FlushThld", ErrInvalidOptions)
	}
	if opts.SyncThld <= 0 {
		return fmt.Errorf("%w: invalid index option SyncThld", ErrInvalidOptions)
	}
	if opts.FlushBufferSize <= 0 {
		return fmt.Errorf("%w: invalid index option FlushBufferSize", ErrInvalidOptions)
	}
	if opts.CleanupPercentage < 0 || opts.CleanupPercentage > 100 {
		return fmt.Errorf("%w: invalid index option CleanupPercentage", ErrInvalidOptions)
	}
	if opts.MaxActiveSnapshots <= 0 {
		return fmt.Errorf("%w: invalid index option MaxActiveSnapshots", ErrInvalidOptions)
	}
	if opts.MaxNodeSize <= 0 {
		return fmt.Errorf("%w: invalid index option MaxNodeSize", ErrInvalidOptions)
	}
	if opts.CompactionThld <= 0 {
		return fmt.Errorf("%w: invalid index option CompactionThld", ErrInvalidOptions)
	}
	if opts.DelayDuringCompaction < 0 {
		return fmt.Errorf("%w: invalid index option DelayDuringCompaction", ErrInvalidOptions)
	}
	if opts.RenewSnapRootAfter < 0 {
		return fmt.Errorf("%w: invalid index option RenewSnapRootAfter", ErrInvalidOptions)
	}
	if opts.MaxBulkSize < 1 {
		return fmt.Errorf("%w: invalid MaxBulkSize", ErrInvalidOptions)
	}
	if opts.BulkPreparationTimeout < 0 {
		return fmt.Errorf("%w: invalid BulkPreparationTimeout", ErrInvalidOptions)
	}
	if opts.NodesLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid index option NodesLogMaxOpenedFiles", ErrInvalidOptions)
	}
	if opts.HistoryLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid index option HistoryLogMaxOpenedFiles", ErrInvalidOptions)
	}
	if opts.CommitLogMaxOpenedFiles <= 0 {
		return fmt.Errorf("%w: invalid index option CommitLogMaxOpenedFiles", ErrInvalidOptions)
	}

	return nil
}

func (opts *AHTOptions) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil AHT options ", ErrInvalidOptions)
	}
	if opts.WriteBufferSize <= 0 {
		return fmt.Errorf("%w: invalid AHT option WriteBufferSize", ErrInvalidOptions)
	}
	if opts.SyncThld <= 0 {
		return fmt.Errorf("%w: invalid AHT option SyncThld", ErrInvalidOptions)
	}

	return nil
}

func (opts *Options) WithReadOnly(readOnly bool) *Options {
	opts.ReadOnly = readOnly
	return opts
}

func (opts *Options) WithSynced(synced bool) *Options {
	opts.Synced = synced
	return opts
}

func (opts *Options) WithWriteBufferSize(writeBufferSize int) *Options {
	opts.WriteBufferSize = writeBufferSize
	return opts
}

func (opts *Options) WithSyncFrequency(frequency time.Duration) *Options {
	opts.SyncFrequency = frequency
	return opts
}

func (opts *Options) WithFileMode(fileMode os.FileMode) *Options {
	opts.FileMode = fileMode
	return opts
}

func (opts *Options) WithLogger(logger logger.Logger) *Options {
	opts.logger = logger
	return opts
}

func (opts *Options) WithAppFactory(appFactory AppFactoryFunc) *Options {
	opts.appFactory = appFactory
	return opts
}

func (opts *Options) WithAppRemoveFunc(appRemove AppRemoveFunc) *Options {
	opts.appRemove = appRemove
	return opts
}

func (opts *Options) WithCompactionDisabled(disabled bool) *Options {
	opts.CompactionDisabled = disabled
	return opts
}

func (opts *Options) WithMaxActiveTransactions(maxActiveTransactions int) *Options {
	opts.MaxActiveTransactions = maxActiveTransactions
	return opts
}

func (opts *Options) WithMVCCReadSetLimit(mvccReadSetLimit int) *Options {
	opts.MVCCReadSetLimit = mvccReadSetLimit
	return opts
}

func (opts *Options) WithMaxConcurrency(maxConcurrency int) *Options {
	opts.MaxConcurrency = maxConcurrency
	return opts
}

func (opts *Options) WithMaxIOConcurrency(maxIOConcurrency int) *Options {
	opts.MaxIOConcurrency = maxIOConcurrency
	return opts
}

func (opts *Options) WithMaxTxEntries(maxTxEntries int) *Options {
	opts.MaxTxEntries = maxTxEntries
	return opts
}

func (opts *Options) WithMaxKeyLen(maxKeyLen int) *Options {
	opts.MaxKeyLen = maxKeyLen
	return opts
}

func (opts *Options) WithMaxValueLen(maxValueLen int) *Options {
	opts.MaxValueLen = maxValueLen
	return opts
}

func (opts *Options) WithTxLogCacheSize(txLogCacheSize int) *Options {
	opts.TxLogCacheSize = txLogCacheSize
	return opts
}

func (opts *Options) WithVLogCacheSize(vLogCacheSize int) *Options {
	opts.VLogCacheSize = vLogCacheSize
	return opts
}

func (opts *Options) WithFileSize(fileSize int) *Options {
	opts.FileSize = fileSize
	return opts
}

func (opts *Options) WithVLogMaxOpenedFiles(vLogMaxOpenedFiles int) *Options {
	opts.VLogMaxOpenedFiles = vLogMaxOpenedFiles
	return opts
}

func (opts *Options) WithTxLogMaxOpenedFiles(txLogMaxOpenedFiles int) *Options {
	opts.TxLogMaxOpenedFiles = txLogMaxOpenedFiles
	return opts
}

func (opts *Options) WithCommitLogMaxOpenedFiles(commitLogMaxOpenedFiles int) *Options {
	opts.CommitLogMaxOpenedFiles = commitLogMaxOpenedFiles
	return opts
}

func (opts *Options) WithMaxWaitees(maxWaitees int) *Options {
	opts.MaxWaitees = maxWaitees
	return opts
}

func (opts *Options) WithTimeFunc(timeFunc TimeFunc) *Options {
	opts.TimeFunc = timeFunc
	return opts
}

func (opts *Options) WithExternalCommitAllowance(useExternalCommitAllowance bool) *Options {
	opts.UseExternalCommitAllowance = useExternalCommitAllowance
	return opts
}

func (opts *Options) WithMultiIndexing(multiIndexing bool) *Options {
	opts.MultiIndexing = multiIndexing
	return opts
}

func (opts *Options) WithWriteTxHeaderVersion(version int) *Options {
	opts.WriteTxHeaderVersion = version
	return opts
}

func (opts *Options) WithCompressionFormat(compressionFormat int) *Options {
	opts.CompressionFormat = compressionFormat
	return opts
}

func (opts *Options) WithCompresionLevel(compressionLevel int) *Options {
	opts.CompressionLevel = compressionLevel
	return opts
}

func (opts *Options) WithEmbeddedValues(embeddedValues bool) *Options {
	opts.EmbeddedValues = embeddedValues
	return opts
}

func (opts *Options) WithPreallocFiles(preallocFiles bool) *Options {
	opts.PreallocFiles = preallocFiles
	return opts
}

func (opts *Options) WithIndexOptions(indexOptions *IndexOptions) *Options {
	opts.IndexOpts = indexOptions
	return opts
}

func (opts *Options) WithAHTOptions(ahtOptions *AHTOptions) *Options {
	opts.AHTOpts = ahtOptions
	return opts
}

func (opts *Options) WithDiscardPrecommittedTransactions(discard bool) *Options {
	opts.DiscardPrecommittedTransactions = discard
	return opts
}

// IndexOptions

func (opts *IndexOptions) WithCacheSize(cacheSize int) *IndexOptions {
	opts.CacheSize = cacheSize
	return opts
}

func (opts *IndexOptions) WithFlushThld(flushThld int) *IndexOptions {
	opts.FlushThld = flushThld
	return opts
}

func (opts *IndexOptions) WithSyncThld(syncThld int) *IndexOptions {
	opts.SyncThld = syncThld
	return opts
}

func (opts *IndexOptions) WithFlushBufferSize(flushBufferSize int) *IndexOptions {
	opts.FlushBufferSize = flushBufferSize
	return opts
}

func (opts *IndexOptions) WithCleanupPercentage(cleanupPercentage float32) *IndexOptions {
	opts.CleanupPercentage = cleanupPercentage
	return opts
}

func (opts *IndexOptions) WithMaxActiveSnapshots(maxActiveSnapshots int) *IndexOptions {
	opts.MaxActiveSnapshots = maxActiveSnapshots
	return opts
}

func (opts *IndexOptions) WithMaxNodeSize(maxNodeSize int) *IndexOptions {
	opts.MaxNodeSize = maxNodeSize
	return opts
}

func (opts *IndexOptions) WithRenewSnapRootAfter(renewSnapRootAfter time.Duration) *IndexOptions {
	opts.RenewSnapRootAfter = renewSnapRootAfter
	return opts
}

func (opts *IndexOptions) WithMaxBulkSize(maxBulkSize int) *IndexOptions {
	opts.MaxBulkSize = maxBulkSize
	return opts
}

func (opts *IndexOptions) WithBulkPreparationTimeout(bulkPreparationTimeout time.Duration) *IndexOptions {
	opts.BulkPreparationTimeout = bulkPreparationTimeout
	return opts
}

func (opts *IndexOptions) WithCompactionThld(compactionThld int) *IndexOptions {
	opts.CompactionThld = compactionThld
	return opts
}

func (opts *IndexOptions) WithDelayDuringCompaction(delayDuringCompaction time.Duration) *IndexOptions {
	opts.DelayDuringCompaction = delayDuringCompaction
	return opts
}

func (opts *IndexOptions) WithNodesLogMaxOpenedFiles(nodesLogMaxOpenedFiles int) *IndexOptions {
	opts.NodesLogMaxOpenedFiles = nodesLogMaxOpenedFiles
	return opts
}

func (opts *IndexOptions) WithHistoryLogMaxOpenedFiles(historyLogMaxOpenedFiles int) *IndexOptions {
	opts.HistoryLogMaxOpenedFiles = historyLogMaxOpenedFiles
	return opts
}

func (opts *IndexOptions) WithCommitLogMaxOpenedFiles(commitLogMaxOpenedFiles int) *IndexOptions {
	opts.CommitLogMaxOpenedFiles = commitLogMaxOpenedFiles
	return opts
}

func (opts *IndexOptions) WithMaxBufferedDataSize(size int) *IndexOptions {
	opts.MaxBufferedDataSize = size
	return opts
}

func (opts *IndexOptions) WithMaxGlobalBufferedDataSize(size int) *IndexOptions {
	opts.MaxGlobalBufferedDataSize = size
	return opts
}

// AHTOptions

func (opts *AHTOptions) WithWriteBufferSize(writeBufferSize int) *AHTOptions {
	opts.WriteBufferSize = writeBufferSize
	return opts
}

func (opts *AHTOptions) WithSyncThld(syncThld int) *AHTOptions {
	opts.SyncThld = syncThld
	return opts
}
