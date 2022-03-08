#include <cstring>
#include <cctype>
#include <atomic>
#include <thread>

#include "utils.h"
#include "index.h"

typedef uint64_t keytype;
typedef std::less<uint64_t> keycomp;

static const uint64_t value_type=1; // 0 = random pointers, 1 = pointers to keys
// Whether we only perform insert
static bool insert_only = false;

template <typename Fn, typename... Args>
void StartThreads(Index<keytype, keycomp> *tree_p,
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
// LOAD
//==============================================================
inline void load(std::vector<keytype> &init_keys, 
                 std::vector<keytype> &keys, 
                 std::vector<uint64_t> &values, 
                 std::vector<int> &ranges, 
                 std::vector<int> &ops) {
  std::string init_file;
  std::string txn_file;

  init_file = "dataset.dat";
  txn_file = "query.dat";

  std::ifstream infile_load(init_file);
  if(!infile_load) {
    fprintf(stderr, "query.dat is not found");
    exit(-1);
  }

  std::string op;
  std::string val;
  keytype key;
  int range;

  std::string insert("INSERT");
  std::string read("READ");
  std::string update("UPDATE");
  std::string scan("SCAN");

  int count = 0;
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
  
  fprintf(stderr, "Loaded %d keys\n", count);

  count = 0;
  uint64_t value = 0;
  void *base_ptr = malloc(8);
  uint64_t base = (uint64_t)(base_ptr);
  free(base_ptr);

  keytype *init_keys_data = init_keys.data();

  if (value_type == 0) {
    while (count < init_keys.size()) {
      value = base + rand();
      values.push_back(value);
      count++;
    }
  }
  else {
    while (count < init_keys.size()) {
      values.push_back(reinterpret_cast<uint64_t>(init_keys_data+count));
      count++;
    }
  }

  // If we do not perform other transactions, we can skip txn file
  if(insert_only == true) {
    return;
  }

  // If we also execute transaction then open the 
  // transacton file here
  std::ifstream infile_txn(txn_file);
  if(!infile_txn) {
    fprintf(stderr, "query.dat is not found");
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
}

//==============================================================
// EXEC
//==============================================================
inline void exec(int index_type, 
                 int num_thread,
                 std::vector<keytype> &init_keys, 
                 std::vector<keytype> &keys, 
                 std::vector<uint64_t> &values, 
                 std::vector<int> &ranges, 
                 std::vector<int> &ops) {

  Index<keytype, keycomp> *idx = getInstance<keytype, keycomp>(index_type);

  //WRITE ONLY TEST-----------------
  int count = (int)init_keys.size();
  //fprintf(stderr, "Populating the index with %d keys using %d threads\n", count, num_thread);

  auto func = [idx, &init_keys, num_thread, &values, index_type] \
              (uint64_t thread_id, bool) {
    size_t total_num_key = init_keys.size();
    size_t key_per_thread = total_num_key / num_thread;
    size_t start_index = key_per_thread * thread_id;
    size_t end_index = start_index + key_per_thread;
#ifdef INTERLEAVED_INSERT
    for(size_t i = thread_id;i < total_num_key; i += num_thread) {
#else
    for(size_t i = start_index;i < end_index;i++) {
#endif
        idx->insert(init_keys[i], values[i]);
    } 
    return;
  };
 
  double start_time = get_now(); 
  StartThreads(idx, num_thread, func, false);
  double end_time = get_now();
  
  double tput = count / (end_time - start_time) / 1000000; //Mops/sec
  std::cout << "insert " << tput << "Mops/s" << "\n";

  // If we do not perform other transactions, we can skip txn file
  if(insert_only == true) {
    return;
  }

  //READ/UPDATE/SCAN TEST----------------
  int txn_num = ops.size();
  uint64_t sum = 0;
  uint64_t s = 0;

  if(values.size() < keys.size()) {
    fprintf(stderr, "Values array too small\n");
    exit(1);
  }

  //fprintf(stderr, "# of Txn: %d\n", txn_num);

  // This is used to count how many read misses we have found
  std::atomic<size_t> read_miss_counter{}, read_hit_counter{};
  read_miss_counter.store(0UL);
  read_hit_counter.store(0UL);

  auto func2 = [num_thread, 
                idx, index_type, 
                &read_miss_counter,
                &read_hit_counter,
                &keys,
                &values,
                &ranges,
                &ops](uint64_t thread_id, bool) {
    size_t total_num_op = ops.size();
    size_t op_per_thread = total_num_op / num_thread;
    size_t start_index = op_per_thread * thread_id;
    size_t end_index = start_index + op_per_thread;
   
    uint64_t v;

    int counter = 0;
    for(size_t i = start_index;i < end_index;i++) {
      int op = ops[i];
      if (op == OP_INSERT) { //INSERT
        idx->insert(keys[i], values[i]);
      } else if (op == OP_READ) { //READ
        idx->find(keys[i], &v);
      } else if (op == OP_UPSERT) { //UPDATE
        idx->upsert(keys[i], reinterpret_cast<uint64_t>(&keys[i]));
      } else if (op == OP_SCAN) { //SCAN
        idx->scan(keys[i], ranges[i]);
      }
    }
    return;
  };

  start_time = get_now();  
  StartThreads(idx, num_thread, func2, false);
  end_time = get_now();

  tput = txn_num / (end_time - start_time) / 1000000; //Mops/sec
  std::cout << "query " << tput << "Mops/s" << "\n";
  delete idx;
  return;
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    std::cout << "Usage:\n";
    std::cout << "1. index type: alex artolc hydralist\n";
    std::cout << "2. number of threads (integer)\n";
    return 1;
  }

  int index_type;
  if (strcmp(argv[1], "artolc") == 0)
    index_type = TYPE_ARTOLC;
  else if(strcmp(argv[1], "alex") == 0) 
    index_type = TYPE_ALEX;
  else if(strcmp(argv[1], "btree") == 0)
    index_type = TYPE_STXBTREE;
  else if(strcmp(argv[1], "wotree") == 0)
    index_type = TYPE_MORPHTREE_WO;
  else {
    fprintf(stderr, "Unknown index type: %d\n", index_type);
    exit(1);
  }

  // Then read number of threads using command line
  int num_thread = atoi(argv[2]);
  if(num_thread < 1 || num_thread > 500) {
    fprintf(stderr, "Do not support %d threads\n", num_thread);
    exit(1);
  } 
  // else {
  //   fprintf(stderr, "Number of threads: %d\n", num_thread);
  // }

  // Then read all remianing arguments
  char **argv_end = argv + argc;
  for(char **v = argv + 3;v != argv_end;v++) {
    if(strcmp(*v, "--insert-only") == 0) {
      insert_only = true;
    } else {
      fprintf(stderr, "Unknown switch: %s\n", *v);
      exit(1);
    }
  }

  if(insert_only == true) {
    fprintf(stderr, "Program will exit after insert operation\n");
  }

  std::vector<keytype> init_keys;
  std::vector<keytype> keys;
  std::vector<uint64_t> values;
  std::vector<int> ranges;
  std::vector<int> ops; //INSERT = 0, READ = 1, UPDATE = 2, SCAN = 3

  init_keys.reserve(100000000);
  keys.reserve(10000000);
  values.reserve(10000000);
  ranges.reserve(10000000);
  ops.reserve(10000000);

  memset(&init_keys[0], 0x00, 100000000 * sizeof(keytype));
  memset(&keys[0], 0x00, 10000000 * sizeof(keytype));
  memset(&values[0], 0x00, 10000000 * sizeof(uint64_t));
  memset(&ranges[0], 0x00, 10000000 * sizeof(int));
  memset(&ops[0], 0x00, 10000000 * sizeof(int));

  load(init_keys, keys, values, ranges, ops);
  //printf("Finished loading workload file (mem = %lu)\n", MemUsage());
  exec(index_type, num_thread, init_keys, keys, values, ranges, ops);
  //printf("Finished running benchmark (mem = %lu)\n", MemUsage());

  return 0;
}