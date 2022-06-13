#include <cstring>
#include <cctype>
#include <atomic>
#include <thread>
#include <cassert>

#include "utils.h"
#include "index.h"

#define WARMUP true

typedef double KeyType;
typedef uint64_t ValType;

static bool insert_only = false;
static bool detail_tp = false;
static const int INTERVAL = 1000000;

template <typename Fn, typename... Args>
void StartThreads(Index<KeyType, ValType> *tree_p,
                  uint64_t num_threads,
                  Fn &&fn,
                  Args &&...args) {
  std::vector<std::thread> thread_group;

  auto fn2 = [tree_p, &fn](uint64_t thread_id, Args ...args) {
    fn(thread_id, args...);
    return;
  };

  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread{fn2, thread_itr, std::ref(args...)});
  }

  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  return;
}

//==============================================================
// LOAD DATA FROM FILE
//==============================================================
void load(std::vector<KeyType> &init_keys, std::vector<KeyType> &keys, 
          std::vector<int> &ranges, std::vector<int> &ops) {
  std::string init_file;
  std::string txn_file;

  init_file = "../build/dataset.dat";
  txn_file = "../build/query.dat";

  std::ifstream infile_load(init_file);
  if(!infile_load) {
    fprintf(stderr, "dataset.dat is not found\n");
    exit(-1);
  }

  std::string op;
  std::string val;
  KeyType key;
  int range;

  std::string insert("INSERT");
  std::string read("READ");
  std::string update("UPDATE");
  std::string scan("SCAN");

  size_t count = 0;
  while (true) {
    infile_load >> op >> key;
    if(!infile_load.good()) 
      break;
      
    if (op.compare(insert) != 0) {
      std::cout << "READING LOAD FILE FAIL!\n";
      return;
    }
    init_keys.push_back(key);
    count++;
  }

  // If we do not perform other transactions, we can skip txn file
  if(insert_only == true) {
    return;
  }

  // If we also execute transaction then open the 
  // transacton file here
  std::ifstream infile_txn(txn_file);
  if(!infile_txn) {
    fprintf(stderr, "query.dat is not found\n");
    exit(-1);
  }
  
  count = 0;
  while (true) {
    infile_txn >> op >> key;
    if(!infile_txn.good()) {
      break;
    }

    if (op.compare(insert) == 0) {
      ops.push_back(OP_INSERT);
      keys.push_back(key);
      ranges.push_back(1);
    }
    else if (op.compare(read) == 0) {
      ops.push_back(OP_READ);
      keys.push_back(key);
    }
    else if (op.compare(update) == 0) {
      ops.push_back(OP_UPSERT);
      keys.push_back(key);
    }
    else if (op.compare(scan) == 0) {
      infile_txn >> range;
      ops.push_back(OP_SCAN);
      keys.push_back(key);
      ranges.push_back(range);
    }
    else {
      std::cout << "UNRECOGNIZED CMD!\n";
      return;
    }
    count++;
  }

  infile_load.close();
  infile_txn.close();
}

//==============================================================
// POPULATE THE INDEX
//==============================================================
Index<KeyType, ValType> * populate(int index_type, std::vector<KeyType> &init_keys) {
  Index<KeyType, ValType> *idx = getInstance<KeyType, ValType>(index_type);
  
  uint64_t bulkload_size = init_keys.size() / 4;

  if (index_type == TYPE_ALEX || index_type == TYPE_LIPP) {
    std::pair<KeyType, uint64_t> *recs;
    recs = new std::pair<KeyType, uint64_t>[bulkload_size];

    // sort the keys
    std::vector<KeyType> bulk_keys;
    for(int i = 0; i < bulkload_size; i++) {
      bulk_keys.push_back(init_keys[i]);
    }
    std::sort(bulk_keys.begin(), bulk_keys.end());
    
    // generate records
    for (int i = 0; i < bulkload_size; i++) {
      recs[i] = {bulk_keys[i], ValType(std::ceil(bulk_keys[i]))};
    }

    idx->bulkload(recs, bulkload_size);
    delete recs;
  } else {
    for(size_t i = 0; i < bulkload_size; i++) {
      idx->insert(init_keys[i], ValType(std::ceil(init_keys[i])));
    } 
  }
 
  double start_time = get_now(); 
  size_t total_num_key = init_keys.size() - bulkload_size;
  size_t end_index = bulkload_size + total_num_key;
  for(size_t i = bulkload_size; i < end_index;i++) {
    idx->insert(init_keys[i], ValType(std::ceil(init_keys[i])));
  } 
  double end_time = get_now();
  
  double tput = (init_keys.size() - bulkload_size) / (end_time - start_time) / 1000000; //Mops/sec
  // if(detail_tp == false)
  //   std::cout << tput << "\t";
  return idx;
}

//==============================================================
// EXEC
//==============================================================
void exec(int index_type, 
                 int num_thread,
                 std::vector<KeyType> &init_keys, 
                 std::vector<KeyType> &keys, 
                 std::vector<int> &ranges, 
                 std::vector<int> &ops) {
  Index<KeyType, ValType> *idx = populate(index_type, init_keys);
  
  // If we do not perform other transactions, we can skip txn file
  if(insert_only == true) {
    idx->printTree();
    return;
  }
  
  #ifdef WARMUP
    size_t warmup_size = ops.size() / 10;
  #else
    size_t warmup_size = 0;
  #endif

  // warmup part
  uint64_t v;
  for(size_t i = 0; i < warmup_size; i++) {
      int op = ops[i];
      if (op == OP_INSERT) { //INSERT
        idx->insert(keys[i], uint64_t(keys[i]));
      } else if (op == OP_READ) { //READ
        bool found = idx->find(keys[i], &v);
        // assert(found == true);
      } else if (op == OP_UPSERT) { //UPDATE
        idx->upsert(keys[i], reinterpret_cast<uint64_t>(&keys[i]));
      } else if (op == OP_SCAN) { //SCAN
        idx->scan(keys[i], ranges[i]);
      }
  }
  
  // test part
  auto func2 = [num_thread, 
                idx, index_type, warmup_size,
                &keys,
                &ranges,
                &ops](uint64_t thread_id, bool) {
    size_t txn_num = ops.size() - warmup_size;
    size_t op_per_thread = txn_num / num_thread;
    size_t start_index = warmup_size + op_per_thread * thread_id;
    size_t end_index = start_index + op_per_thread;

    uint64_t v;
    int counter = 0;
    double last_ts = get_now(), cur_ts;
    for(size_t i = start_index;i < end_index;i++) {
      int op = ops[i];
      if (op == OP_INSERT) { //INSERT
        idx->insert(keys[i], uint64_t(keys[i]));
      } else if (op == OP_READ) { //READ
        bool found = idx->find(keys[i], &v);
        // assert(found == true);
      } else if (op == OP_UPSERT) { //UPDATE
        idx->upsert(keys[i], reinterpret_cast<uint64_t>(&keys[i]));
      } else if (op == OP_SCAN) { //SCAN
        idx->scan(keys[i], ranges[i]);
      }

      if(detail_tp == true && (i + 1) % INTERVAL == 0) {
        cur_ts = get_now();
        double tput = INTERVAL / (cur_ts - last_ts) / 1000000; //Mops/sec
        std::cout << tput << std::endl;
        last_ts = cur_ts;
      }
    }
    return;
  };

  auto start_time = get_now();  
  func2(0, false);
  auto end_time = get_now();

  double tput = (ops.size() - warmup_size) / (end_time - start_time) / 1000000; //Mops/sec
  if(detail_tp == false)
    std::cout << tput << " ";
    // std::cout  << tput << " " << std::endl;
  
  delete idx;
  return;
}

int main(int argc, char *argv[]) {
  if (argc == 1) {
    std::cout << "Usage:\n";
    std::cout << "1. index type \n";
    std::cout << "2. number of threads (integer)\n";
    return 1;
  }

  int index_type;
  if(strcmp(argv[1], "alex") == 0) 
    index_type = TYPE_ALEX;
  else if(strcmp(argv[1], "lipp") == 0)
    index_type = TYPE_LIPP;
  else if(strcmp(argv[1], "wotree") == 0)
    index_type = TYPE_MORPHTREE_WO;
  else if(strcmp(argv[1], "rotree") == 0)
    index_type = TYPE_MORPHTREE_RO;
  else if(strcmp(argv[1], "morphtree") == 0)
    index_type = TYPE_MORPHTREE;
  else if(strcmp(argv[1], "rotree2") == 0)
    index_type = TYPE_NEWTREE;
  else {
    fprintf(stderr, "Unknown index type: %d\n", index_type);
    exit(1);
  }

  int num_thread = 1;
  if(argc >= 3 && atoi(argv[2]) >= 1) {
    num_thread = atoi(argv[2]);
  }

  std::vector<KeyType> init_keys;
  std::vector<KeyType> keys;
  std::vector<int> ranges;
  std::vector<int> ops; //INSERT = 0, READ = 1, UPDATE = 2, SCAN = 3

  init_keys.reserve(64000000);
  keys.reserve(64000000);
  ops.reserve(64000000);

  memset(&init_keys[0], 0x00, 64000000 * sizeof(KeyType));
  memset(&keys[0], 0x00, 64000000 * sizeof(KeyType));
  memset(&ops[0], 0x00, 64000000 * sizeof(int));

  if(detail_tp) {
    printf("index type: %d\n", index_type);
  }

  load(init_keys, keys, ranges, ops);
  // fprintf(stderr, "finish loading\n");
  exec(index_type, num_thread, init_keys, keys, ranges, ops);
  return 0;
}