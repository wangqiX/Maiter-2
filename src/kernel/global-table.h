#ifndef GLOBALTABLE_H_
#define GLOBALTABLE_H_

#include "table.h"
#include "local-table.h"

#include "util/file.h"
#include "util/rpc.h"

#include <queue>

//#define GLOBAL_TABLE_USE_SCOPEDLOCK

DECLARE_int32(bufmsg);

namespace dsm {

class Worker;
class Master;

// Encodes table entries using the passed in TableData protocol buffer.
struct ProtoTableCoder : public TableCoder {
	ProtoTableCoder(const TableData* in);
  virtual void WriteEntryToFile(StringPiece k, StringPiece v1, StringPiece v2, StringPiece v3);
  virtual bool ReadEntryFromFile(string* k, string *v1, string *v2, string *v3);

  int read_pos_;
  TableData *t_;
};

// Encodes table entries using the passed in TableData protocol buffer.
struct ProtoKVPairCoder : public KVPairCoder {
	ProtoKVPairCoder(const KVPairData* in);
  virtual void WriteEntryToNet(StringPiece k, StringPiece v1);
  virtual bool ReadEntryFromNet(string* k, string *v1);

  int read_pos_;
  KVPairData *t_;
};

struct PartitionInfo {
  PartitionInfo() : dirty(false), tainted(false) {}
  bool dirty;
  bool tainted;
  ShardInfo sinfo;
};

class GlobalTable : virtual public TableBase {
public:
  virtual void UpdatePartitions(const ShardInfo& sinfo) = 0;
  virtual TableIterator* get_iterator(int shard, bool bfilter, unsigned int fetch_num = FETCH_NUM) = 0;

  virtual bool is_local_shard(int shard) = 0;
  //virtual bool is_master_vertex(int key) = 0;
  virtual bool is_local_key(const StringPiece &k) = 0;

  virtual PartitionInfo* get_partition_info(int shard) = 0;
  virtual LocalTable* get_partition(int shard) = 0;

  virtual bool tainted(int shard) = 0;
  virtual int owner(int shard) = 0;

protected:
  friend class Worker;
  friend class Master;

  virtual int64_t shard_size(int shard) = 0;
};

class MutableGlobalTable : virtual public GlobalTable {
public:
  // Handle updates from the master or other workers.
  virtual void SendUpdates() = 0;
  virtual void ApplyUpdates(const KVPairData& req) = 0;
  virtual void HandlePutRequests() = 0;
  virtual void TermCheck() = 0;

  virtual int pending_write_bytes() = 0;

  virtual void clear() = 0;
  virtual void resize(int64_t new_size) = 0;

  // Exchange the content of this table with that of table 'b'.
  virtual void swap(GlobalTable *b) = 0;
protected:
  friend class Worker;
  virtual void local_swap(GlobalTable *b) = 0;
};

class GlobalTableBase : virtual public GlobalTable {
public:
  virtual ~GlobalTableBase();

  void Init(const TableDescriptor *tinfo);

  void UpdatePartitions(const ShardInfo& sinfo);

  virtual TableIterator* get_iterator(int shard, bool bfilter, unsigned int fetch_num = FETCH_NUM) = 0;

  virtual bool is_local_shard(int shard);
  virtual bool is_local_key(const StringPiece &k);
  
  int64_t shard_size(int shard);

  PartitionInfo* get_partition_info(int shard) { return &partinfo_[shard]; }
  LocalTable* get_partition(int shard) { return partitions_[shard]; }

  bool tainted(int shard) { return get_partition_info(shard)->tainted; }
  int owner(int shard) { return get_partition_info(shard)->sinfo.owner(); }
protected:
  virtual int shard_for_key_str(const StringPiece& k) = 0;

  int worker_id_;

  vector<LocalTable*> partitions_;
  vector<LocalTable*> cache_;

  boost::recursive_mutex& mutex() { return m_; }
  boost::recursive_mutex m_;
  boost::mutex& trigger_mutex() { return m_trig_; }
  boost::mutex m_trig_;

  vector<PartitionInfo> partinfo_;

  struct CacheEntry {
    double last_read_time;
    string value;
  };

  unordered_map<StringPiece, CacheEntry> remote_cache_;
};

class MutableGlobalTableBase :
  virtual public GlobalTableBase,
  virtual public MutableGlobalTable,
  virtual public Checkpointable {
public:
  MutableGlobalTableBase() {
        pending_writes_ = 0;
        snapshot_index = 0;
        sent_bytes_ = 0;
  }

  void BufSend();
  void SendUpdates();
  virtual void ApplyUpdates(const KVPairData& req) = 0;
  void HandlePutRequests();
  void TermCheck();

  int pending_write_bytes();

  void clear();
  void resize(int64_t new_size);

  void start_checkpoint(const string& f);
  void write_delta(const KVPairData& d);
  void finish_checkpoint();
  void restore(const string& f);  

  void swap(GlobalTable *b);

  int64_t sent_bytes_;
  Timer timer;
  int timerindex;
  
protected:
  int64_t pending_writes_;
  int snapshot_index;
  void local_swap(GlobalTable *b);
  void termcheck();


	//double send_overhead;
	//double objectcreate_overhead;
	//int sendtime;
};

template <class K, class V1, class V2, class V3>
class TypedGlobalTable :
  virtual public GlobalTable,
  public MutableGlobalTableBase,
  public TypedTable<K, V1, V2, V3>,
  private boost::noncopyable {
public:
  virtual ~TypedGlobalTable(){}

  bool initialized(){
	return binit;
  }

  typedef pair<K, V1> KVPair;
  typedef TypedTableIterator<K, V1, V2, V3> Iterator;
  typedef NetDecodeIterator<K, V1> NetUpdateDecoder;
  virtual void Init(const TableDescriptor *tinfo) {
    GlobalTableBase::Init(tinfo);

    for (int i = 0; i < partitions_.size(); ++i) {
    	partitions_[i] = create_deltaT(i);
    }
    //Clear the update queue, just in case
    update_queue.clear();
    binit = false;
  }

  void InitStateTable(){
    for (int i = 0; i < partitions_.size(); ++i) {
        if(is_local_shard(i)){
            partitions_[i] = create_localT(i);
        }
    }
    binit = true;
  }
  
  
  
  
  

  int get_shard(const K& k);
  V1 get_localF1(const K& k);
  V2 get_localF2(const K& k);
  V3 get_localF3(const K& k);

  // Store the given key-value pair in this hash. If 'k' has affinity for a
  // remote thread, the application occurs immediately on the local host,
  // and the update is queued for transmission to the owner.
	void put(const K &k, const V1 &v1, const V2 &v2, const V3 &v3, const int &v4, const map<int,V1> &v5,const int &master);
	void updateF1(const K &k, const V1 &v,const int& shard);
	void updateF2(const K &k, const V2 &v,const int& shard);
	void updateF3(const K &k, const V3 &v);
        void updateF4(const K &k, const int &v);
        void updateF5(const K &k, const V3 &v);
	void enqueue_updateF1(K k, V1 v);
	void accumulateF1(const K &k, const V1 &v,const int& source,const int& shard,const bool& is_SendMessage); // 2 TypeGloobleTable :TypeTable
	void accumulateDelta(const K &k,const V1& v,const int& shard);
        void accumulateF2(const K &k, const V2 &v,const int& shard);
	void accumulateF3(const K &k, const V3 &v);
        void accumulateF4(const K &k, const int &v);
        void accumulateF5(const K &k, const V3 &v);
	// Return the value associated with 'k', possibly blocking for a remote fetch.
	ClutterRecord<K, V1, V2, V3> get(const K &k);
	V1 getF1(const K &k);
	V2 getF2(const K &k);
	V3 getF3(const K &k);
        int getF4(const K &k);
        map<int,V1> getF5(const K &k);
        int getmaster(const K& k);
	bool contains(const K &k);
	bool remove(const K &k);
        int get_size(const int shard);//获取顶点的个数
  
  TableIterator* get_iterator(int shard, bool bfilter, unsigned int fetch_num = FETCH_NUM);
  
  TypedTable<K, V1, V2, V3>* partition(int idx) {
    return dynamic_cast<TypedTable<K, V1, V2, V3>* >(partitions_[idx]);
  }

  PTypedTable<K, V1, V3>* deltaT(int idx) {
    return dynamic_cast<PTypedTable<K, V1, V3>* >(partitions_[idx]);
  }

  virtual TypedTableIterator<K, V1, V2, V3>* get_typed_iterator(int shard, bool bfilter, unsigned int fetch_num = FETCH_NUM) {
    return static_cast<TypedTableIterator<K, V1, V2, V3>* >(get_iterator(shard, bfilter, fetch_num));
  }
  
  TypedTableIterator<K, V1, V2, V3>* get_entirepass_iterator(int shard) {
    return (TypedTableIterator<K, V1, V2, V3>*) partitions_[shard]->entirepass_iterator(this->helper());
  }
    
  void ApplyUpdates(const dsm::KVPairData& req) {
    boost::recursive_mutex::scoped_lock sl(mutex());

    VLOG(2) << "applying updates, from " << req.source();

    if (!is_local_shard(req.shard())) {
      LOG_EVERY_N(INFO, 1000)
          << "Forwarding push request from: " << MP(id(), req.shard())
          << " to " << owner(req.shard());
    }

    // Changes to support centralized of triggers <CRM>
    ProtoKVPairCoder c(&req);
    NetUpdateDecoder it;
    partitions_[req.shard()]->deserializeFromNet(&c, &it);

    for(;!it.done(); it.Next()) {
    	VLOG(2) << this->owner(req.shard()) << ":" << req.shard() << "read from remote " << it.key() << ";" << it.value1();
        accumulateF1(it.key(),it.value1(),req.source(),req.shard(),false);//消息的接收
        
    }
    //TermCheck();
  }

  Marshal<K> *kmarshal() { return ((Marshal<K>*)info_.key_marshal); }
  Marshal<V1> *v1marshal() { return ((Marshal<V1>*)info_.value1_marshal); }
  Marshal<V2> *v2marshal() { return ((Marshal<V2>*)info_.value2_marshal); }
  Marshal<V3> *v3marshal() { return ((Marshal<V3>*)info_.value3_marshal); }
  Marshal<int> *v4marshal() { return ((Marshal<int>*)info_.value4_marshal); }
//  Marshal<V3> *v5marshal() { return ((Marshal<V3>*)info_.value5_marshal); }
protected:
  int shard_for_key_str(const StringPiece& k);
  virtual LocalTable* create_localT(int shard);
  virtual LocalTable* create_deltaT(int shard);
  deque<KVPair> update_queue;
  bool binit;
};

static const int kWriteFlushCount = 1000000;

template<class K, class V1, class V2, class V3>
int TypedGlobalTable<K, V1, V2, V3>::get_shard(const K& k) {
  DCHECK(this != NULL);
  DCHECK(this->info().sharder != NULL);

  Sharder<K> *sharder = (Sharder<K>*)(this->info().sharder);//对()进行了重载
  int shard = (*sharder)(k, this->info().num_shards);
  DCHECK_GE(shard, 0);
  DCHECK_LT(shard, this->num_shards());
  return shard;
}

template<class K, class V1, class V2, class V3>
int TypedGlobalTable<K, V1, V2, V3>::shard_for_key_str(const StringPiece& k) {
  return get_shard(unmarshal(static_cast<Marshal<K>* >(this->info().key_marshal), k));
}

template<class K, class V1, class V2, class V3>
V1 TypedGlobalTable<K, V1, V2, V3>::get_localF1(const K& k) {
  int shard = this->get_shard(k);

  CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

  return partition(shard)->getF1(k);
}

template<class K, class V1, class V2, class V3>
V2 TypedGlobalTable<K, V1, V2, V3>::get_localF2(const K& k) {
  int shard = this->get_shard(k);

  CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

  return partition(shard)->getF2(k);
}

template<class K, class V1, class V2, class V3>
V3 TypedGlobalTable<K, V1, V2, V3>::get_localF3(const K& k) {
  int shard = this->get_shard(k);

  CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

  return partition(shard)->getF3(k);
}

// Store the given key-value pair in this hash. If 'k' has affinity for a
// remote thread, the application occurs immediately on the local host,
// and the update is queued for transmission to the owner.
template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::put(const K &k, const V1 &v1, const V2 &v2, const V3 &v3, const int &v4, const map<int,V1> &v5,const int &master){
  int shard = this->info_.helper->id();//应该是传递current_shard,当前是认为shard = worker 待规范化

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
    boost::recursive_mutex::scoped_lock sl(mutex());
#endif
     partition(shard)->put(k, v1, v2, v3,v4,v5,master);
    
//  if (is_local_shard(shard)) {
//      partition(shard)->put(k, v1, v2, v3,v4,v5,ismaster);
//  }else{
//	  VLOG(1) << "not local put";
//	  ++pending_writes_;
//  }

  //if (pending_writes_ > kWriteFlushCount) {
    //SendUpdates();
  //}
  //BufSend();

  PERIODIC(0.1, {this->HandlePutRequests();});
}
template<class K, class V1, class V2, class V3>
int TypedGlobalTable<K, V1, V2, V3>::get_size(const int shard) {

  CHECK(is_local_shard(shard)) << " non-local for shard: " << shard;

  return partition(shard)->get_size(shard);
}


//###
//template<class K, class V1, class V2, class V3>
//void TypedGlobalTable<K, V1, V2, V3>::updateF1(const K &k, const V1 &v) {
//  int shard = this->get_shard(k);
//
//#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
//    boost::mutex::scoped_lock sl(trigger_mutex());
//    boost::recursive_mutex::scoped_lock sl(mutex());
//#endif
//
//  if (is_local_shard(shard)) {
//      partition(shard)->updateF1(k, v);
//
//    //VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
//  } else {
//      deltaT(shard)->update(k, v);
//
//  }
//  PERIODIC(0.1, {this->HandlePutRequests();});
//}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::updateF1(const K &k, const V1 &v,const int& shard) {

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
    boost::mutex::scoped_lock sl(trigger_mutex());
    boost::recursive_mutex::scoped_lock sl(mutex());
#endif
      partition(shard)->updateF1(k, v,shard);
      //PERIODIC(0.1, {this->HandlePutRequests();});//周期性的接受消息
}


template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::updateF2(const K &k, const V2 &v,const int& shard) {

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
    boost::mutex::scoped_lock sl(trigger_mutex());
    boost::recursive_mutex::scoped_lock sl(mutex());
#endif

  if (is_local_shard(shard)) {
      partition(shard)->updateF2(k, v,shard);

    //VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
  } else {
	  VLOG(2) << "update F2 shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
  }
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::updateF3(const K &k, const V3 &v) {
  int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
    boost::mutex::scoped_lock sl(trigger_mutex());
    boost::recursive_mutex::scoped_lock sl(mutex());
#endif

  if (is_local_shard(shard)) {
      partition(shard)->updateF3(k, v);

    //VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
  } else {
	  VLOG(2) << "update F3 shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
  }
}
template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::updateF4(const K &k, const int &v) {
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::updateF5(const K &k, const V3 &v) {
}

//###
//template<class K, class V1, class V2, class V3>
//void TypedGlobalTable<K, V1, V2, V3>::accumulateF1(const K &k, const V1 &v) { //3
//  int shard = this->get_shard(k);
//
//#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
//    boost::mutex::scoped_lock sl(trigger_mutex());
//    boost::recursive_mutex::scoped_lock sl(mutex());
//#endif
//
//  if (is_local_shard(shard)) {
//       //VLOG(1) << this->owner(shard) << ":" << shard << " accumulate " << v << " on local " << k;
//      partition(shard)->accumulateF1(k, v);  //TypeTable
//  } else {
//        //VLOG(1) << this->owner(shard) << ":" << shard << " accumulate " << v << " on remote " << k;
//        deltaT(shard)->accumulate(k, v);
//
//        ++pending_writes_;
//        if (pending_writes_ > FLAGS_bufmsg) {
//            SendUpdates();
//        }
//        //BufSend();
//        //PERIODIC(0.1, {this->HandlePutRequests();});
//  }
//}
template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::accumulateDelta(const K &k,const V1& v,const int& shard){
   deltaT(shard)->accumulate(k, v);
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::accumulateF1(const K &k, const V1 &v,const int& source,const int& shard,const bool& is_SendMessage) { //3
   //source为消息的源partition  shard为顶点的当前partition
    //对于本地消息source用于
#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
    boost::mutex::scoped_lock sl(trigger_mutex());
    boost::recursive_mutex::scoped_lock sl(mutex());
#endif
    TypedTable<K, V1, V2, V3>* statetable_ = partition(shard);//这句最费时间
   
//  bool is_master_vertex = is_local_shard(this->get_shard(k));//这一句还是挺耗时的
//  if (is_master_vertex) {//该副本顶点是master_vertex,则向各个mirrors_vertex发送数据
   //statetable_->getF4(k);//7.7+0.8=8.5
    
     statetable_->accumulateF1(k, v,source,shard,true);//只对计算顶点累积
  
   
   //if(k==92283370)VLOG(0)<<"1worker="<<this->info_.helper->id()<<" source="<<source<<" shard="<<shard<<endl;
   //if(k==92283370&&is_SendMessage)VLOG(0)<<"2worker="<<this->info_.helper->id()<<" source="<<source<<" shard="<<shard<<endl;
   int master=statetable_->getmaster(k);
   if(is_SendMessage){//本地消息
       if(shard!=master){//master顶点
           deltaT(master)->accumulate(k, v);
       }
   }
   else{//远程消息
        if(shard==master){//master顶点
           statetable_->accumulateSBuffer(k,source,v);//对master的顶点进行缓存
       }
   }  

}



//###
//template<class K, class V1, class V2, class V3>
//void TypedGlobalTable<K, V1, V2, V3>::accumulateF2(const K &k, const V2 &v,const int& shard) { // 1
//  int shard = in
//  this->get_shard(k);
//
//#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
//    boost::mutex::scoped_lock sl(trigger_mutex());
//    boost::recursive_mutex::scoped_lock sl(mutex());
//#endif
//
//  if (is_local_shard(shard)) {
//      partition(shard)->accumulateF2(k, v); // Typetable
//
//    //VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
//  } else {
//	  VLOG(2) << "accumulate F2 shard " << shard << " local? " << " : " << is_local_shard(shard);
//  }
//}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::accumulateF2(const K &k, const V2 &v,const int& shard) { // 1
    //int k_master = this->get_shard(k);//只对master_vertex进行计算  形参shard暂时没有使用
#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
    boost::mutex::scoped_lock sl(trigger_mutex());
    boost::recursive_mutex::scoped_lock sl(mutex());
#endif
    if(partition(shard)->getmaster(k)==shard) {
        partition(shard)->accumulateF2(k, v,shard); // Typetable
      //VLOG(0) << " worker " << shard << " : " << k << " update";
  } 
//  else {
//	  VLOG(2) << "accumulate F2 shard " << shard << " local? " << " : " << is_local_shard(shard);
//  }
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::accumulateF3(const K &k, const V3 &v) {
  int shard = this->get_shard(k);

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
    boost::mutex::scoped_lock sl(trigger_mutex());
    boost::recursive_mutex::scoped_lock sl(mutex());
#endif

  if (is_local_shard(shard)) {
      partition(shard)->accumulateF3(k, v);

    //VLOG(3) << " shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
  } else {
	  VLOG(2) << "accumulate F3 shard " << shard << " local? " << " : " << is_local_shard(shard) << " : " << worker_id_;
  }
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::accumulateF4(const K &k, const int &v) {
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::accumulateF5(const K &k, const V3 &v) {
}

template<class K, class V1, class V2, class V3>
void TypedGlobalTable<K, V1, V2, V3>::enqueue_updateF1(K k, V1 v) {
  const KVPair thispair(k,v);
  update_queue.push_back(thispair);
}



// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V1, class V2, class V3>
ClutterRecord<K, V1, V2, V3> TypedGlobalTable<K, V1, V2, V3>::get(const K &k) {
  int shard = this->get_shard(k);

  CHECK_EQ(is_local_shard(shard), true) << "key " << k << " is not located in local table";
#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
  return partition(shard)->get(k);
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V1, class V2, class V3>
V1 TypedGlobalTable<K, V1, V2, V3>::getF1(const K &k) {
  int shard = this->get_shard(k);

  CHECK_EQ(is_local_shard(shard), true) << "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
    return partition(shard)->getF1(k);
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V1, class V2, class V3>
V2 TypedGlobalTable<K, V1, V2, V3>::getF2(const K &k) {
  int shard = this->get_shard(k);

  CHECK_EQ(is_local_shard(shard), true) << "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
    return partition(shard)->getF2(k);
}

// Return the value associated with 'k', possibly blocking for a remote fetch.
template<class K, class V1, class V2, class V3>
V3 TypedGlobalTable<K, V1, V2, V3>::getF3(const K &k) {
  int shard = this->get_shard(k);

  CHECK_EQ(is_local_shard(shard), true) << "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
    return partition(shard)->getF3(k);
}

template<class K, class V1, class V2, class V3>
int TypedGlobalTable<K, V1, V2, V3>::getF4(const K &k) {
  int shard = this->get_shard(k);
  CHECK_EQ(is_local_shard(shard), true) << "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
    return partition(shard)->getF4(k);
}

template<class K, class V1, class V2, class V3>
map<int,V1> TypedGlobalTable<K, V1, V2, V3>::getF5(const K &k) {
  int shard = this->get_shard(k);
  CHECK_EQ(is_local_shard(shard), true) << "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
    return partition(shard)->getF5(k);
}
template<class K, class V1, class V2, class V3>
int TypedGlobalTable<K, V1, V2, V3>::getmaster(const K &k) {
  int shard = this->get_shard(k);
    //int shard =this->info_.helper->id();
     //if(k==92283370)VLOG(0)<<"worker="<<shard<<endl;
  CHECK_EQ(is_local_shard(shard), true) << "key " << k << " is not located in local table";

#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
    return partition(shard)->getmaster(k);
}


template<class K, class V1, class V2, class V3>
bool TypedGlobalTable<K, V1, V2, V3>::contains(const K &k) {
  int shard = this->get_shard(k);

  if (is_local_shard(shard)) {
#ifdef GLOBAL_TABLE_USE_SCOPEDLOCK
        boost::recursive_mutex::scoped_lock sl(mutex());
#endif
    return partition(shard)->contains(k);
  }else {
	  return false;
  }
}

template<class K, class V1, class V2, class V3>
bool TypedGlobalTable<K, V1, V2, V3>::remove(const K &k) {
	  int shard = this->get_shard(k);

	  if (is_local_shard(shard)) {
	    return partition(shard)->remove(k);
	    return true;
	  }else {
		  return false;
	  }
}

template<class K, class V1, class V2, class V3>
LocalTable* TypedGlobalTable<K, V1, V2, V3>::create_localT(int shard) {
  TableDescriptor *linfo = new TableDescriptor(info());
  linfo->shard = shard;
  VLOG(2) << "create local statetable " << shard;
  LocalTable* t = (LocalTable*)info_.partition_factory->New();
  t->Init(linfo);

  return t;
}

template<class K, class V1, class V2, class V3>
LocalTable* TypedGlobalTable<K, V1, V2, V3>::create_deltaT(int shard) {
  TableDescriptor *linfo = new TableDescriptor(info());
  linfo->shard = shard;
  VLOG(2) << "create local deltatable " << shard;
  LocalTable* t = (LocalTable*)info_.deltaT_factory->New();
  t->Init(linfo);

  return t;
}

template<class K, class V1, class V2, class V3>
TableIterator* TypedGlobalTable<K, V1, V2, V3>::get_iterator(int shard, bool bfilter, unsigned int fetch_num) {
      CHECK_EQ(this->is_local_shard(shard), true) << "should use local get_iterator";

      if(info().schedule_portion < 1){;
              return (TypedTableIterator<K, V1, V2, V3>*) partitions_[shard]->schedule_iterator(this->helper(), bfilter);
      }else{
              return (TypedTableIterator<K, V1, V2, V3>*) partitions_[shard]->get_iterator(this->helper(), bfilter);
      }
}

}

#endif /* GLOBALTABLE_H_ */
