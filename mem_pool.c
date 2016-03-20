/*
 * Created by Ivo Georgiev on 2/9/16.
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;



/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt poolMgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt poolMgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt poolMgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt poolMgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt poolMgr);
static alloc_status _add_gap(pool_mgr_pt poolMgr, node_pt node);
static node_pt _add_node(pool_mgr_pt poolMgr, node_pt prevNode);
static node_pt _convert_gap(pool_mgr_pt poolMgr, gap_pt gap, size_t size);
static void _sortGap(gap_pt gapIX, int lower, int higher);
/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
    // ensure that it's called only once until mem_free
    // allocate the pool store with initial capacity
    // note: holds pointers only, other functions to allocate/deallocate

	if (!pool_store) {
		pool_store = (pool_mgr_pt *) calloc(MEM_POOL_STORE_INIT_CAPACITY, sizeof(pool_mgr_t));

		if (pool_store == NULL) {
			printf("Pool store allocation failed.");
			return ALLOC_FAIL;
		}
		else {
			pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
			pool_store_size = 0;
			return ALLOC_OK;
		}
	} else {
		return ALLOC_CALLED_AGAIN;
	}


    return ALLOC_FAIL;
}

alloc_status mem_free() {
    // ensure that it's called only once for each mem_init
    // make sure all pool managers have been deallocated
    // can free the pool store array
    // update static variables
	if (!pool_store) {
		return ALLOC_CALLED_AGAIN;
	} else {
		for (unsigned int i = 0; i < pool_store_size; i++) {
			mem_pool_close((pool_pt)pool_store[i]);
		}
		free(pool_store);
		pool_store_size = 0;
		pool_store_capacity = 0;
		pool_store = NULL;
		return ALLOC_OK;
	}

    return ALLOC_FAIL;
}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {
    // make sure there the pool store is allocated
    // expand the pool store, if necessary
    // allocate a new mem pool mgr
    // check success, on error return null
    // allocate a new memory pool
    // check success, on error deallocate mgr and return null
    // allocate a new node heap
    // check success, on error deallocate mgr/pool and return null
    // allocate a new gap index
    // check success, on error deallocate mgr/pool/heap and return null
    // assign all the pointers and update meta data:
    //   initialize top node of node heap
    //   initialize top node of gap index
    //   initialize pool mgr
    //   link pool mgr to pool store
    // return the address of the mgr, cast to (pool_pt)

	if (!pool_store) {
		alloc_status status = mem_init();
		if (status != ALLOC_OK) return NULL;
	} else {
		pool_mgr_pt poolMgr = (pool_mgr_pt) calloc(1,sizeof(pool_mgr_t));//TODO
		if (!poolMgr){
			return NULL;
		} else {
			poolMgr->pool.alloc_size = 0;
			poolMgr->pool.total_size = size;
			poolMgr->pool.policy = policy;
			poolMgr->pool.num_gaps = 0;
			poolMgr->pool.num_allocs = 0;
			poolMgr->pool.mem = (char*) malloc(size);	//malloc
			if (!poolMgr->pool.mem) {
				free(poolMgr);
				return NULL;
			}
			//Node Heap Allocation
			poolMgr->node_heap = (node_pt) calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));
			poolMgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
			poolMgr->used_nodes = 0;
			if (!poolMgr->node_heap){
				free(poolMgr->pool.mem);
				free(poolMgr);
				return NULL;
			}
			//Gap Index Allocation
			poolMgr->gap_ix = (gap_pt) calloc(MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));
			poolMgr->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;
			if (!poolMgr->gap_ix){
				free(poolMgr->pool.mem);
				free(poolMgr->node_heap);
				free(poolMgr);
				return NULL;
			}
			//Allocate first node
			const node_pt head = _add_node(poolMgr, NULL);
			head->alloc_record.mem = poolMgr->pool.mem;
			head->alloc_record.size = poolMgr->pool.total_size;
			head->next = NULL;
			head->prev = NULL;
			head->used = 1;
			head->allocated = 1;
			
			//Add head
			_add_gap(poolMgr, head);
			pool_store[pool_store_size] = poolMgr;
			pool_store_size++;
			return (pool_pt)poolMgr;
		}
	}
    return NULL;
}

alloc_status mem_pool_close(pool_pt pool) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    // check if this pool is allocated
    // check if pool has only one gap
    // check if it has zero allocations
    // free memory pool
    // free node heap
    // free gap index
    // find mgr in pool store and set to null
    // note: don't decrement pool_store_size, because it only grows
    // free mgr
	const pool_mgr_pt poolMgr = (pool_mgr_pt)pool;
	if(poolMgr == NULL){
		//printf("Failed to find pool\n");
		return ALLOC_FAIL;
	} else {
		for (unsigned int i=0; i< poolMgr->used_nodes; i++) {
			if (poolMgr->node_heap[i].allocated) {
				return ALLOC_NOT_FREED;
			}
		}
		if (poolMgr->pool.num_allocs <= 0) {
			//return ALLOC_NOT_FREED;
		}
		for (unsigned int i = 0; i < pool_store_size; i++) {
			if (pool_store[i] == poolMgr) {
				pool_store[i] = NULL;
			}
		}
		free(poolMgr->pool.mem);
		free(poolMgr->gap_ix);
		free(poolMgr->node_heap);
		free(poolMgr);
		return ALLOC_OK;
	}
	printf("Failed to close pool\n");
    return ALLOC_FAIL;
}

alloc_pt mem_new_alloc(pool_pt pool, size_t size) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
	const pool_mgr_pt poolMgr = (pool_mgr_pt)pool;
    // check if any gaps, return null if none
	if (poolMgr->pool.num_gaps < 1) return NULL;
    // expand heap node, if necessary, quit on error
	if (poolMgr->total_nodes > MEM_NODE_HEAP_INIT_CAPACITY*MEM_NODE_HEAP_FILL_FACTOR) {
		
	}
    // check used nodes fewer than total nodes, quit on error
	if (poolMgr->total_nodes < poolMgr->used_nodes) {
		return NULL;
	}
    // get a node for allocation
    // if FIRST_FIT, then find the first sufficient node in the node heap
    // if BEST_FIT, then find the first sufficient node in the gap index
    // check if node found
    // update metadata (num_allocs, alloc_size)
    // calculate the size of the remaining gap, if any
    // remove node from gap index
    // convert gap_node to an allocation node of given size
    // adjust node heap:
    //   if remaining gap, need a new node
    //   find an unused one in the node heap
    //   make sure one was found
    //   initialize it to a gap node
    //   update metadata (used_nodes)
    //   update linked list (new node right after the node for allocation)
    //   add to gap index
    //   check if successful
    // return allocation record by casting the node to (alloc_pt)
	
	node_pt new = NULL;
	node_pt best = NULL;	
	node_pt current = poolMgr->node_heap;

	if (poolMgr->pool.policy == FIRST_FIT) {
		while (current) {
			if (current->used == 1 && current->allocated == 0) {
				if (current->alloc_record.size >= size) {
					best = current;
					break;
				}
			}
			current = current->next;
		}
	}

	if (poolMgr->pool.policy == BEST_FIT) {
		while (current) {
			if (current->used == 1 && current->allocated == 0) {
				if(current->alloc_record.size >= size) {
					if (best != NULL) {
						if (current->alloc_record.size < best->alloc_record.size) {
							best = current;
						}
					} else {
						best = current;
					}
				}
			}
			current = current->next;
		}
	}
	if (best != NULL) {
		for (unsigned int i = 0; i < poolMgr->pool.num_gaps; i++) {
			if (poolMgr->gap_ix[i].node == best) {
				new = (node_pt) _convert_gap(poolMgr, &(poolMgr->gap_ix[i]), size);
				break;
			}
		}
	}
	if (new) {
		poolMgr->pool.num_allocs++;
		poolMgr->pool.alloc_size += size;
		return &(new->alloc_record);
	} 

    return NULL;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
	const pool_mgr_pt poolMgr = (pool_mgr_pt)pool;
    // get node from alloc by casting the pointer to (node_pt)
	const node_pt node = (node_pt)alloc;
    // find the node in the node heap
	node_pt current = poolMgr->node_heap;
	node_pt del = NULL;
	while (current->next) {
		if (current == node) {
			del = current;
			break; 
		}
		current = current->next;
	}
    // this is node-to-delete
    // make sure it's found
	if (del!=NULL) {
		del->used = 0;
    // convert to gap node
    // update metadata (num_allocs, alloc_size)
    // if the next node in the list is also a gap, merge into node-to-delete
    //   remove the next node from gap index
    //   check success
    //   add the size to the node-to-delete
    //   update node as unused
    //   update metadata (used nodes)
    //   update linked list:
    /*
                    if (next->next) {
                        next->next->prev = node_to_del;
                        node_to_del->next = next->next;
                    } else {
                        node_to_del->next = NULL;
                    }
                    next->next = NULL;
                    next->prev = NULL;
     */

    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // if the previous node in the list is also a gap, merge into previous!
    //   remove the previous node from gap index
    //   check success
    //   add the size of node-to-delete to the previous
    //   update node-to-delete as unused
    //   update metadata (used_nodes)
    //   update linked list
    /*
                    if (node_to_del->next) {
                        prev->next = node_to_del->next;
                        node_to_del->next->prev = prev;
                    } else {
                        prev->next = NULL;
                    }
                    node_to_del->next = NULL;
                    node_to_del->prev = NULL;
     */
    //   change the node to add to the previous node!
    // add the resulting node to the gap index
    // check success

	/*if (_add_gap(poolMgr, node) == ALLOC_OK){
		printf("mem del alloc >>add gap>> succeeded/n");
		poolMgr->pool.num_allocs--;
		poolMgr->pool.alloc_size -= alloc->size;
		return ALLOC_OK;
	}*/
    return ALLOC_FAIL;
}

void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    // get the mgr from the pool
	const pool_mgr_pt poolMgr = (pool_mgr_pt)pool;
    // allocate the segments array with size == used_nodes
	*segments = (pool_segment_pt) calloc(poolMgr->used_nodes, sizeof(pool_segment_t));
    // check successful
	if (!segments){
		return;
	}
    // loop through the node heap and the segments array
	node_pt current = poolMgr->node_heap;
	unsigned int index = 0;
	while(current) {
		if (current->used == 1) {
			(*segments)[index].size = current->alloc_record.size;
			(*segments)[index].allocated = current->allocated;
			index++;
		}
		current = current->next;
	}
	*num_segments = index;
	return;
    //    for each node, write the size and allocated in the segment
    // "return" the values:
    /*
                    *segments = segs;
                    *num_segments = pool_mgr->used_nodes;
     */
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {
    // check if necessary
    /*
                if (((float) pool_store_size / pool_store_capacity)
                    > MEM_POOL_STORE_FILL_FACTOR) {...}
     */
    // don't forget to update capacity variables
	if (pool_store_size < pool_store_capacity * MEM_POOL_STORE_FILL_FACTOR) {
		return ALLOC_OK;
	}
	pool_mgr_pt * tempMgr = (pool_mgr_pt*) realloc(pool_store, pool_store_capacity * MEM_POOL_STORE_EXPAND_FACTOR * sizeof(pool_mgr_pt));	//TODO
	
	if (tempMgr != NULL) {
		pool_store = tempMgr;
		pool_store_capacity *= MEM_POOL_STORE_EXPAND_FACTOR;
		return ALLOC_OK;
	}

    return ALLOC_FAIL;
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt poolMgr) {
    // see above
	if (poolMgr->used_nodes < poolMgr->total_nodes * MEM_NODE_HEAP_FILL_FACTOR) {
		return ALLOC_OK;
	}

	node_pt tempNode = (node_pt) realloc(poolMgr->node_heap, poolMgr->total_nodes * MEM_NODE_HEAP_EXPAND_FACTOR * sizeof(node_t));
	if (tempNode != NULL) {
		poolMgr->node_heap = tempNode;
		poolMgr->total_nodes *= MEM_NODE_HEAP_EXPAND_FACTOR;
		return ALLOC_OK;
	}
    return ALLOC_FAIL;
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt poolMgr) {
    // see above
	if (poolMgr->gap_ix_capacity < poolMgr->gap_ix_capacity * MEM_GAP_IX_FILL_FACTOR) {
		return ALLOC_OK;
	} else {
		gap_pt temp = (gap_pt) realloc(poolMgr->gap_ix, poolMgr->gap_ix_capacity *MEM_GAP_IX_EXPAND_FACTOR *sizeof(gap_t));
		if (temp != NULL) {
			poolMgr->gap_ix = temp;
			poolMgr->gap_ix_capacity *= MEM_GAP_IX_EXPAND_FACTOR;
			return ALLOC_OK;
		} 
    return ALLOC_FAIL;
	}
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt poolMgr,
                                       size_t size,
                                       node_pt node) {
    // expand the gap index, if necessary (call the function)
	alloc_status stat = _mem_resize_gap_ix(poolMgr);
    // add the entry at the end
    // update metadata (num_gaps)
    // sort the gap index (call the function)
    // check success
	
    return _add_gap(poolMgr, node);
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt poolMgr,
                                            size_t size,
                                            node_pt node) {
    // find the position of the node in the gap index
	node_pt current = poolMgr->node_heap;
	unsigned int pos = 0;
	for (unsigned int i = 0; i < poolMgr->pool.num_gaps; i++){
		if (poolMgr->gap_ix[i].node == node) {
			pos = i;
		}
	}
    // loop from there to the end of the array:
    //    pull the entries (i.e. copy over) one position up
    //    this effectively deletes the chosen node
	for (unsigned int j = pos; j < poolMgr->pool.num_gaps; j++){
		poolMgr->gap_ix[j] = poolMgr->gap_ix[j+1];
	}
    // update metadata (num_gaps)
	poolMgr->pool.num_gaps--;
    // zero out the element at position num_gaps!
	poolMgr->gap_ix[poolMgr->pool.num_gaps].node = NULL;
    // sort the new gap index
	_mem_sort_gap_ix(poolMgr);
    return ALLOC_FAIL;
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt poolMgr) {
	// Sort using a recursive quicksort method
	_sortGap(poolMgr->gap_ix, 0, poolMgr->pool.num_gaps - 1);
	//Check success
	for (unsigned int i = 0; i < poolMgr->pool.num_gaps; i++) {
		if (poolMgr->gap_ix[0].size > poolMgr->gap_ix[i].size) return ALLOC_FAIL;
	}
    return ALLOC_OK;
}

static void _sortGap(gap_pt gapIX, int lower, int higher) {
	unsigned int i, j;
	if (lower < higher) {
		gap_pt pivot = &(gapIX[lower]);
		gap_t result;
		i = lower; 
		j = higher +1;
		while (i < j) {
			while (gapIX[i].size <= pivot->size && i <= higher) {
				i++;
			}
			while (gapIX[j].size >pivot->size){
				j--;
			}
			result = gapIX[i];
			gapIX[i] = gapIX[j];
			gapIX[j] = result;
		}
		result = gapIX[lower];
		gapIX[lower] = gapIX[j];
		gapIX[j] = result;
		_sortGap(gapIX,lower, j-1); //Sort lower half
		_sortGap(gapIX,j+1, higher); //Sort upper
	}
}

static alloc_status _add_gap(pool_mgr_pt poolMgr, node_pt node) {
	gap_pt gap = NULL;
	char* endAdd = (char*) (node->alloc_record.mem + node->alloc_record.size); //Find ending address to prevent conflicts
//Merge gaps below
	for (unsigned int i = 0; i < poolMgr->pool.num_gaps; i++) {
		if (endAdd == poolMgr->gap_ix[i].node->alloc_record.mem) {
			node_pt del = poolMgr->gap_ix[i].node;
			del->used = 0;
			if (del->prev != NULL) {
				del->prev->next = del->next;
			} else {
				del->next->prev = NULL;
			}
			if (del->next != NULL) {
				del->next->prev = del->prev;
			} else {
				del->prev->next = NULL;
			}
			poolMgr->gap_ix[i].node = node;
			node->allocated = 0;
			poolMgr->gap_ix[i].size += node->alloc_record.size;
			poolMgr->gap_ix[i].node->alloc_record.size = poolMgr->gap_ix[i].size;
			gap = &(poolMgr->gap_ix[i]);
			break;
		}
	}
//Merge gaps above
	for (unsigned int i = 0; i < poolMgr->pool.num_gaps; i++) {
		node_pt current = poolMgr->gap_ix[i].node;
		char* gapEnd = (char*) (current->alloc_record.mem + current->alloc_record.size);
		if (node->alloc_record.mem == gapEnd) {
			node_pt del = node;
			del->used = 0;
			if (del->prev != NULL) {
				del->prev->next = del->next;
			} else {
				del->next->prev = NULL;
			}
			if (del->next != NULL) {
				del->next->prev = del->prev;
			} else {
				del->prev->next = NULL;
			}
			node->allocated = 0;
			poolMgr->gap_ix[i].node->allocated = 0;
			poolMgr->gap_ix[i].size += node->alloc_record.size;
			poolMgr->gap_ix[i].node->alloc_record.size = poolMgr->gap_ix[i].size;
//POSSIBLE SAVIOUR
			gap = &(poolMgr->gap_ix[i]);
			break;
		}
	}
//No neighboring gaps for merging
	//Try to resize gap index
	if (_mem_resize_gap_ix(poolMgr) != ALLOC_OK) return ALLOC_FAIL;
	poolMgr->pool.num_gaps++;
	const gap_pt new = &(poolMgr->gap_ix[poolMgr->pool.num_gaps - 1]);
	new->size = node->alloc_record.size;
	new->node = node;
	node->allocated = 0;
	_mem_sort_gap_ix(poolMgr);
	return ALLOC_OK;
} 

static node_pt _add_node(pool_mgr_pt poolMgr, node_pt prevNode) {
	//Try to find or make space	
	if (_mem_resize_node_heap(poolMgr) != ALLOC_OK) return NULL;
	const node_pt new = &(poolMgr->node_heap[poolMgr->used_nodes]);
	poolMgr->used_nodes++;
	new->next = NULL;
	new->prev = NULL;
	new->allocated = 0;
	new->alloc_record.mem = NULL;
	new->alloc_record.size = 0;
	if (poolMgr->used_nodes == 1) { // only one in heap
		return new;
	} else {
		if (prevNode != NULL) { // previous node exists
			new->prev = prevNode;
			if (prevNode->next != NULL) {
				new->next = prevNode->next;
			}
			prevNode->next = new;
		} else { // next node exists
			node_pt end = poolMgr->node_heap;
			while (end->next) end = end->next;
			end->next = new;
			new->prev = end;
		}
		return new;
	}
}

static node_pt _convert_gap(pool_mgr_pt poolMgr, gap_pt gap, size_t size) {
	const node_pt node = gap->node;	
	//node fits in gap
	if (gap->size == size) {
		node->allocated = 1;
		_mem_remove_from_gap_ix(poolMgr, size, node);
		return node;
	}
	node->allocated = 1;
	node->used = 1;
	node->alloc_record.size = size;
	gap->node = _add_node(poolMgr, node);
	size_t newSize = gap->size - size;
	gap->node->alloc_record.size = newSize;
	gap->size = newSize;

	gap->node->alloc_record.mem = (char*) (node->alloc_record.mem + size);
	gap->node->allocated = 0;
	gap->node->used = 1;
	return node;
}
