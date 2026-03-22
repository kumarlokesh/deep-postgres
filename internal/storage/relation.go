package storage

// Relation is the handle through which all access to a relation's storage is
// mediated.  It binds together the relation's identity (RelFileNode), a shared
// buffer pool, and a storage manager.
//
// This mirrors PostgreSQL's RelationData (src/include/utils/rel.h), greatly
// simplified for single-user research use.
//
// Typical lifecycle:
//
//	smgr := NewFileStorageManager(dataDir)
//	rel := OpenRelation(RelFileNode{DbId: 0, RelId: 1}, pool, smgr)
//	_ = rel.Init()                            // create fork files
//	blk, _ := rel.Extend(ForkMain)            // allocate first block
//	id, _ := rel.ReadBlock(ForkMain, blk)     // pin the page
//	page, _ := rel.Pool.GetPageForWrite(id)
//	page.InsertTuple(data)
//	rel.Pool.UnpinBuffer(id)
//	rel.Flush(ForkMain)                       // fsync before exit

// Relation wraps a RelFileNode with a buffer pool and storage manager.
type Relation struct {
	Node RelFileNode
	Pool *BufferPool
	VM   *VisibilityMap // all-visible bits; nil means VM is disabled
	FSM  *FreeSpaceMap  // free-space estimates per block; nil means disabled
	smgr StorageManager
}

// OpenRelation creates a Relation handle and registers it with the buffer pool
// so that cache misses are served from disk.  It does not create any files;
// call Init or Extend to do that.
func OpenRelation(node RelFileNode, pool *BufferPool, smgr StorageManager) *Relation {
	rel := &Relation{
		Node: node,
		Pool: pool,
		VM:   NewVisibilityMap(0),
		FSM:  NewFreeSpaceMap(0),
		smgr: smgr,
	}
	pool.RegisterRelation(node.RelId, node, smgr)
	return rel
}

// Init creates the storage file for every fork that does not yet exist.
// Safe to call on an already-initialised relation (no-op for existing forks).
func (r *Relation) Init() error {
	for _, fork := range []ForkNumber{ForkMain} {
		if !r.smgr.Exists(r.Node, fork) {
			if err := r.smgr.Create(r.Node, fork); err != nil {
				return err
			}
		}
	}
	return nil
}

// Extend appends one new empty block to fork, writes it to disk, loads it
// into the buffer pool (pinned), initialises the page, and returns the new
// block number together with the buffer ID.
//
// The caller is responsible for calling Pool.UnpinBuffer(id) when done.
func (r *Relation) Extend(fork ForkNumber) (BlockNumber, BufferId, error) {
	blockNum, err := r.smgr.Extend(r.Node, fork)
	if err != nil {
		return InvalidBlockNumber, InvalidBufferId, err
	}

	tag := BufferTag{RelationId: r.Node.RelId, Fork: fork, BlockNum: blockNum}
	id, err := r.Pool.ReadBuffer(tag)
	if err != nil {
		return InvalidBlockNumber, InvalidBufferId, err
	}

	// The block was just created as zeros; initialise it as a proper page.
	page, err := r.Pool.GetPageForWrite(id)
	if err != nil {
		r.Pool.UnpinBuffer(id)
		return InvalidBlockNumber, InvalidBufferId, err
	}
	page.init()
	return blockNum, id, nil
}

// ReadBlock returns a pinned buffer ID for the given fork/block.
// The page is loaded from disk on a cache miss.
// The caller must call Pool.UnpinBuffer(id) when done.
func (r *Relation) ReadBlock(fork ForkNumber, blockNum BlockNumber) (BufferId, error) {
	tag := BufferTag{RelationId: r.Node.RelId, Fork: fork, BlockNum: blockNum}
	return r.Pool.ReadBuffer(tag)
}

// NBlocks returns the current size of fork in blocks.
func (r *Relation) NBlocks(fork ForkNumber) (BlockNumber, error) {
	return r.smgr.NBlocks(r.Node, fork)
}

// Flush syncs all buffered writes for fork to durable storage.
// It first calls FlushAll on the pool (writes dirty pages) then fsyncs.
func (r *Relation) Flush(fork ForkNumber) error {
	r.Pool.FlushAll()
	return r.smgr.Sync(r.Node, fork)
}

// Drop unlinks all fork files for this relation.
func (r *Relation) Drop() error {
	return r.smgr.Unlink(r.Node)
}
