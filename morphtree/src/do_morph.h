#ifndef __DO_MORPH__
#define __DO_MORPH__

#include <cstdint>

#include "node.h"
#include "bucketlog.h"

namespace morphtree {

struct MorphRecord {
    BaseNode * node;
    uint32_t   lsn;
    NodeType   to;
};

// Declaration: variables that controls the morphing of Morphtree
extern uint32_t gacn;
extern bool do_morphing;
extern uint32_t glsn;
extern BucketLog<MorphRecord> morph_log;

}

#endif // __DO_MORPH__