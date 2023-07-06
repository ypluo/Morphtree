#include <cstring>
#include <cctype>
#include <atomic>
#include <thread>
#include <cassert>
#include <unistd.h>

#include <iomanip>
#include <glog/logging.h>

#include "utils.h"
#include "index.h"

int worknum = 0;

typedef double KeyType;
typedef uint64_t ValType;

static bool insert_only = false;
static bool detail_tp = false;
static const int INTERVAL = 2000000;

int get_memory_by_pid(pid_t pid) {
  FILE* fd;
  char line[1024] = {0};
  char virtual_filename[32] = {0};
  char vmrss_name[32] = {0};
  int vmrss_num = 0;
  sprintf(virtual_filename, "/proc/%d/status", pid);
  fd = fopen(virtual_filename, "r");
  if(fd == NULL) {
    std::cout << "open " << virtual_filename << " failed" << std::endl;
    exit(1);
  }

  // VMRSS line is uncertain
  for(int i = 0 ; i < 60; i++) {
    auto discard = fgets(line, sizeof(line), fd);
    if(strstr(line, "VmRSS:") != NULL) {
      sscanf(line, "%s %d", vmrss_name, &vmrss_num);
      break;
    }
  }
  fclose(fd);
  return vmrss_num;
}

//==============================================================
// LOAD DATA FROM FILE
//==============================================================
void load(std::vector<KeyType> &init_keys, std::vector<KeyType> &keys, 
          std::vector<int> &ranges, std::vector<int> &ops) {
  std::string init_file;
  std::string txn_file;

  init_file = "/home/lyp/morphtree/build/dataset.dat";
  txn_file = "/home/lyp/morphtree/build/query.dat";

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
  std::string remove("REMOVE");
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
    ranges.push_back(1);
    if (op.compare(insert) == 0) {
      ops.push_back(OP_INSERT);
      keys.push_back(key);
    }
    else if (op.compare(read) == 0) {
      ops.push_back(OP_READ);
      keys.push_back(key);
    }
    else if (op.compare(update) == 0) {
      ops.push_back(OP_UPSERT);
      keys.push_back(key);
    }
    else if (op.compare(remove) == 0) {
      ops.push_back(OP_REMOVE);
      keys.push_back(key);
    }
    else if (op.compare(scan) == 0) {
      infile_txn >> range;
      ops.push_back(OP_SCAN);
      keys.push_back(key);
      ranges.back() = range;
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
  uint64_t bulkload_size = init_keys.size() / 2;
  if(index_type == TYPE_XINDEX) // bug with xindex populated with single thread
    bulkload_size = init_keys.size();
  // sort the keys
  std::vector<KeyType> bulk_keys;
  for(int i = 0; i < bulkload_size; i++) {
    bulk_keys.push_back(init_keys[i]);
  }
  std::sort(bulk_keys.begin(), bulk_keys.end());
  
  // generate records
  std::pair<KeyType, uint64_t> *recs;
  recs = new std::pair<KeyType, uint64_t>[bulkload_size];
  for (int i = 0; i < bulkload_size; i++) {
    recs[i] = {bulk_keys[i], ValType(std::abs(bulk_keys[i]) + 1)};
  }

  idx->bulkload(recs, bulkload_size);
  delete recs;

  // warmup 
  double start_time = get_now(); 
  size_t total_num_key = init_keys.size() - bulkload_size;
  size_t end_index = bulkload_size + total_num_key;
  for(size_t i = bulkload_size; i < end_index;i++) {
    idx->insert(init_keys[i], ValType(std::abs(init_keys[i] + 1)));
  } 
  double end_time = get_now();
  
  double tput = (init_keys.size() - bulkload_size) / (end_time - start_time) / 1000000; //Mops/sec
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
  
  LOG(INFO) << "finish populate";
  // If we do not perform other transactions, we can skip txn file
  if(insert_only == true) {
    int mem = get_memory_by_pid(getpid());
    printf("%d \n", mem / 1024);
    return;
  }

  // warmup part
  size_t warmup_size = ops.size() / 10;
  ValType v;
  for(size_t i = 0; i < warmup_size; i++) {
      int op = ops[i];
      if (op == OP_INSERT) { //INSERT
        idx->insert(keys[i], ValType(std::abs(keys[i])));
      } else if (op == OP_READ) { //READ
        bool found = idx->find(keys[i], &v);
      } else if (op == OP_UPSERT) { //UPDATE
        idx->upsert(keys[i], ValType(std::abs(keys[i])));
      } else if (op == OP_SCAN) { //SCAN
        idx->scan(keys[i], ranges[i]);
      }
  }
  
  // test part
  auto func2 = [num_thread, 
                idx, index_type, warmup_size,
                &keys,
                &ranges,
                &ops](int thread_id) {
    size_t txn_num = ops.size() - warmup_size;
    size_t op_per_thread = txn_num / num_thread;
    size_t start_index = warmup_size + op_per_thread * thread_id;
    size_t end_index = start_index + op_per_thread;

    Param p = {thread_id};
    uint64_t v;
    int notfound = 0;
    double last_ts = get_now(), cur_ts;
    for(size_t i = start_index;i < end_index;i++) {
      int op = ops[i];
      if (op == OP_INSERT) { //INSERT
        idx->insert(keys[i], ValType(std::abs(keys[i]) + 1), p);
      } else if (op == OP_READ) { //READ
        bool found = idx->find(keys[i], &v, p);
        if(found == false) {
          notfound++;
        }
      } else if (op == OP_UPSERT) { //UPDATE
        idx->upsert(keys[i], ValType(std::abs(keys[i]) + 2), p);
      } else if (op == OP_REMOVE) {
        idx->remove(keys[i], p);
      } else if (op == OP_SCAN) { //SCAN
        idx->scan(keys[i], ranges[i], p);
      }

      if(detail_tp == true && (i + 1) % INTERVAL == 0) {
        cur_ts = get_now();
        double tput = INTERVAL / (cur_ts - last_ts) / 1000000; //Mops/sec
        std::cout << tput << std::endl;
        last_ts = cur_ts;
      }
    }

    LOG(INFO) << "not found number " << notfound;
    return;
  };

  auto start_time = get_now();
  
  std::thread test_threads[num_thread];
  for(int i = 0; i < num_thread; i++)
    test_threads[i] = std::move(std::thread(func2, i));

  for(int i = 0; i < num_thread; i++)
    test_threads[i].join();

  auto end_time = get_now();

  double tput = (ops.size()) / (end_time - start_time) / 1000000; //Mops/sec
  if(detail_tp == false)
    std::cout << tput << std::endl;
  
  delete idx;
  return;
}


int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);

  // google::SetLogDestination(google::GLOG_INFO, "/home/lyp/morphtree/build/log/INFO-");
  // google::SetLogDestination(google::GLOG_ERROR, "/home/lyp/morphtree/build/log/ERROR-");
  google::SetStderrLogging(google::GLOG_ERROR);

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
  else if(strcmp(argv[1], "xindex") == 0) 
    index_type = TYPE_XINDEX;
  else if(strcmp(argv[1], "finedex") == 0)
    index_type = TYPE_FINEDEX;
  else if(strcmp(argv[1], "wotree") == 0)
    index_type = TYPE_MORPHTREE_WO;
  else if(strcmp(argv[1], "rotree") == 0)
    index_type = TYPE_MORPHTREE_RO;
  else if(strcmp(argv[1], "morphtree") == 0)
    index_type = TYPE_MORPHTREE;
  else if(strcmp(argv[1], "btree") == 0)
    index_type = TYPE_BTREE;
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
  std::vector<int> ops; //INSERT = 0, READ = 1, UPDATE = 2, REMOVE=3, SCAN = 4

  init_keys.reserve(64000000);
  keys.reserve(64000000);
  ops.reserve(64000000);

  memset(&init_keys[0], 0x00, 64000000 * sizeof(KeyType));
  memset(&keys[0], 0x00, 64000000 * sizeof(KeyType));
  memset(&ops[0], 0x00, 64000000 * sizeof(int));

  LOG(INFO) << "index type:" << index_type;
  LOG(INFO) << "thread num:" << num_thread;
  
  load(init_keys, keys, ranges, ops);

  if(insert_only == true) {
    int mem = get_memory_by_pid(getpid());
    printf("%d ", mem / 1024);
  }

  worknum = num_thread;
  exec(index_type, num_thread, init_keys, keys, ranges, ops);

  google::ShutdownGoogleLogging();
  return 0;
}
