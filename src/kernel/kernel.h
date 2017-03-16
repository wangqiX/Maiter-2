#ifndef KERNELREGISTRY_H_
#define KERNELREGISTRY_H_

#include "kernel/table.h"
#include "kernel/global-table.h"
#include "kernel/local-table.h"
#include "kernel/table-registry.h"


#include "util/common.h"
#include <boost/function.hpp>
#include <boost/lexical_cast.hpp>

DECLARE_string(graph_dir);

namespace dsm {

template <class K, class V1, class V2, class V3>
class TypedGlobalTable;

class TableBase;
class Worker;

template <class K, class V, class D>
class MaiterKernel;

#ifndef SWIG
class MarshalledMap {
public:
  struct MarshalledValue {
    virtual string ToString() const = 0;
    virtual void FromString(const string& s) = 0;
    virtual void set(const void* nv) = 0;
    virtual void* get() const = 0;
  };

  template <class T>
  struct MarshalledValueT  : public MarshalledValue {
    MarshalledValueT() : v(new T) {}
    ~MarshalledValueT() { delete v; }

    string ToString() const {
      string tmp;
      m_.marshal(*v, &tmp);
      return tmp;
    }

    void FromString(const string& s) {
      m_.unmarshal(s, v);
    }

    void* get() const { return v; }
    void set(const void *nv) {
      *v = *(T*)nv;
    }

    mutable Marshal<T> m_;
    T *v;
  };

  template <class T>
  void put(const string& k, const T& v) {
    if (serialized_.find(k) != serialized_.end()) {
      serialized_.erase(serialized_.find(k));
    }

    if (p_.find(k) == p_.end()) {
      p_[k] = new MarshalledValueT<T>;
    }

    p_[k]->set(&v);
  }

  template <class T>
  T& get(const string& k) const {
    if (serialized_.find(k) != serialized_.end()) {
      p_[k] = new MarshalledValueT<T>;
      p_[k]->FromString(serialized_[k]);
      serialized_.erase(serialized_.find(k));
    }

    return *(T*)p_.find(k)->second->get();
  }

  bool contains(const string& key) const {
    return p_.find(key) != p_.end() ||
           serialized_.find(key) != serialized_.end();
  }

  Args* ToMessage() const {
    Args* out = new Args;
    for (unordered_map<string, MarshalledValue*>::const_iterator i = p_.begin(); i != p_.end(); ++i) {
      Arg *p = out->add_param();
      p->set_key(i->first);
      p->set_value(i->second->ToString());
    }
    return out;
  }

  // We can't immediately deserialize the parameters passed in, since sadly we don't
  // know the type yet.  Instead, save the string values on the side, and de-serialize
  // on request.
  void FromMessage(const Args& p) {
    for (int i = 0; i < p.param_size(); ++i) {
      serialized_[p.param(i).key()] = p.param(i).value();
    }
  }

private:
  mutable unordered_map<string, MarshalledValue*> p_;
  mutable unordered_map<string, string> serialized_;
};
#endif


class DSMKernel {
public:
  // Called upon creation of this kernel by a worker.
  virtual void InitKernel() {}

  // The table and shard being processed.
  int current_shard() const { return shard_; }
  int current_table() const { return table_id_; }

  template <class T>
  T& get_arg(const string& key) const {
    return args_.get<T>(key);
  }

  template <class T>
  T& get_cp_var(const string& key, T defval=T()) {
    if (!cp_.contains(key)) {
      cp_.put(key, defval);
    }
    return cp_.get<T>(key);
  }

  GlobalTable* get_table(int id);

  template <class K, class V1, class V2, class V3>
  TypedGlobalTable<K, V1, V2, V3>* get_table(int id) {
    return dynamic_cast<TypedGlobalTable<K, V1, V2, V3>*>(get_table(id));
  }
  
  template <class K, class V, class D>
  void set_maiter(MaiterKernel<K, V, D> maiter){}
  
private:
  friend class Worker;
  friend class Master;

  void initialize_internal(Worker* w,
                           int table_id, int shard);

  void set_args(const MarshalledMap& args);
  void set_checkpoint(const MarshalledMap& args);

  Worker *w_;
  int shard_;
  int table_id_;
  MarshalledMap args_;
  MarshalledMap cp_;
};

struct KernelInfo {
  KernelInfo(const char* name) : name_(name) {}

  virtual DSMKernel* create() = 0;
  virtual void Run(DSMKernel* obj, const string& method_name) = 0;
  virtual bool has_method(const string& method_name) = 0;

  string name_;
};

template <class C, class K, class V, class D>
struct KernelInfoT : public KernelInfo {
  typedef void (C::*Method)();
  map<string, Method> methods_;
  MaiterKernel<K, V, D>* maiter;

  KernelInfoT(const char* name, MaiterKernel<K, V, D>* inmaiter) : KernelInfo(name) {
      maiter = inmaiter;
  }

  DSMKernel* create() { return new C; }

  void Run(DSMKernel* obj, const string& method_id) {
    ((C*)obj)->set_maiter(maiter);
    boost::function<void (C*)> m(methods_[method_id]);
    m((C*)obj);
  }

  bool has_method(const string& name) {
    return methods_.find(name) != methods_.end();
  }

  void register_method(const char* mname, Method m, MaiterKernel<K, V, D>* inmaiter) { 
      methods_[mname] = m; 
  }
};

class ConfigData;
class KernelRegistry {
public:
  typedef map<string, KernelInfo*> Map;
  Map& kernels() { return m_; }
  KernelInfo* kernel(const string& name) { return m_[name]; }

  static KernelRegistry* Get();
private:
  KernelRegistry() {}
  Map m_;
};

template <class C, class K, class V, class D>
struct KernelRegistrationHelper {
  KernelRegistrationHelper(const char* name, MaiterKernel<K, V, D>* maiter) {
    KernelRegistry::Map& kreg = KernelRegistry::Get()->kernels();

    CHECK(kreg.find(name) == kreg.end());
    kreg.insert(make_pair(name, new KernelInfoT<C, K, V, D>(name, maiter)));
  }
};


template <class C, class K, class V, class D>
struct MethodRegistrationHelper {
  MethodRegistrationHelper(const char* klass, const char* mname, void (C::*m)(), MaiterKernel<K, V, D>* maiter) {
    ((KernelInfoT<C, K, V, D>*)KernelRegistry::Get()->kernel(klass))->register_method(mname, m, maiter);
  }
};

//template <class K, class V, class D>
//class MaiterKernel0 : public DSMKernel {
//private:
//    MaiterKernel<K, V, D>* maiter;
//public:
//
//
//    void run() {
//        VLOG(0) << "initializing table ";
//        init_table(maiter->table);
//    }
//};

template <class K, class V, class D>
class MaiterKernel1 : public DSMKernel {                    //the first phase: initialize the local state table
private:
    MaiterKernel<K, V, D>* maiter;                          //user-defined iteratekernel
public:
    void set_maiter(MaiterKernel<K, V, D>* inmaiter) {
        maiter = inmaiter;
        }

    void write_data(K key,V delta,V value,D data,int data_property,D mirrors) {
                   
            string file = StringPrintf("%s/part-%d", maiter->output.c_str(), 100);  //the output path
            fstream File;  
            File.open(file.c_str(), ios::out|ios::app);
        
            File << "key=" << key << ", value=" <<value << ", delata=" << delta
                   << ", data_property=" << data_property<< ", data=";
            for (int i = 0; i < data.size(); ++i) {
               File << data[i] << " ";
            }
            File << ", mirrors=";
            for (int i = 0; i < mirrors.size(); ++i) {
                File << mirrors[i] << " ";
            }
            LOG(INFO) << endl;
            File.close();
     }
    
        void show_data(K key,V delta,V value,D data,int data_property,D mirrors) {
                 
            LOG(INFO) << "key=" << key << ", value=" <<value << ", delata=" << delta
                   << ", data_property=" << data_property<< ", data=";
            for (int i = 0; i < data.size(); ++i) {
                LOG(INFO) << data[i] << " ";
            }
            LOG(INFO) << ", mirrors=";
            for (int i = 0; i < mirrors.size(); ++i) {
                LOG(INFO) << mirrors[i] << " ";
            }
            LOG(INFO) << "\n";
      }
     void dump(TypedGlobalTable<K, V, V, D>* a,int step){
        fstream File;                   //the output file containing the local state table infomatio
        string file = StringPrintf("%s/part-%d-%d", maiter->output.c_str(), current_shard(),step);  //the output path
        File.open(file.c_str(), ios::out);
        typename TypedGlobalTable<K, V, V, D>::Iterator *it = a->get_entirepass_iterator(current_shard());
        while(!it->done()) {
                bool cont = it->Next();
                if(!cont) break;
                File << it->position()<<"\t"<<it->key() << "\t"  << it->value2() << "\n";
        }
         //File << "\n";
        delete it;
        File.clear();
        File.close();
    }
    void read_file(TypedGlobalTable<K, V, V, D>* table){
        int shard=current_shard();
        string patition_file = StringPrintf("%s/part%d", FLAGS_graph_dir.c_str(), current_shard());
        ifstream inFile;
        inFile.open(patition_file.c_str());
     
//        double start_time;
//        double time1;
//        double time2;
//        double time3;
//        
//        double totaltime1;
//        double totaltime2;
//        double totaltime3;

        if (!inFile) {
            cerr << "Unable to open file" << patition_file;
            cerr << system("ifconfig -a | grep 192.168.*")<<endl;
            exit(1); // terminate with error
        }        
        char linechr[2024000];//LOG(INFO) << "ok1 ok1 ok1!";
        while (inFile.getline(linechr, 2024000)) {              //read a line of the input file, ensure the buffer is large enough
            K key;
            V delta;
            V value;
            D data;
            int data_property;
            map<int,V> mirrors;//LOG(INFO) << "ok2 ok2 ok2!";
            int master;
            string line(linechr);
            //start_time = Now();
            maiter->iterkernel->read_data(line, key, data,data_property,mirrors,master);   //invoke api, get the value of key field and data field
//            time1= Now();
 
//            totaltime1 +=(time1-start_time);
            maiter->iterkernel->init_v(key,value,data,data_property);          //invoke api, get the initial v field value
     
            //只对master顶点的delta赋值,mirror顶点为0
            if(master==shard){
               maiter->iterkernel->init_c(key, delta,data,1); //invoke api, get the initial delta v field value
            }
            else{
                maiter->iterkernel->init_c(key, delta,data,0);
            }
            

            //            time2= Now();
//            totaltime2 +=(time2-time1);
           // if(shard==0)VLOG(0) << "key="<<key<<endl;
            table->put(key, delta, value, data,data_property,mirrors,master);                //initialize a row of the state table (a node)
            //if(key==415711683)VLOG(0)<<"worker="<<current_shard()<<"master="<<master<<" mirror.size="<<mirrors.size()<<endl;
            
//            time3= Now();
//            totaltime3 +=(time3-time2);
           // write_data(key,delta,value,data,data_property,mirrors);  
        }
      //  VLOG(0) << "part"<<shard<<"的顶点个数是:"<<table->get_size(shard)<< "\n";
       
        
//        LOG(INFO) <<"totaltime1="<<totaltime1<<endl;
//        LOG(INFO) <<"totaltime2="<<totaltime2<<endl;
//        LOG(INFO) <<"totaltime3="<<totaltime3<<endl;
         //LOG(INFO) <<"put_time="<<put_time<<endl;
        
    }

    void init_table(TypedGlobalTable<K, V, V, D>* a){
        if(!a->initialized()){
            a->InitStateTable();        //initialize the local state table
        }
        a->resize(maiter->num_nodes);   //create local state table based on the input size
       read_file(a);                   //initialize the state table fields based on the input data file
       //dump(a,0);//将statetable中的数据输出
    }

    void run() {
        VLOG(0) << "initializing table ";
        init_table(maiter->table);
    }
};

template <class K, class V, class D>
class MaiterKernel2 : public DSMKernel {                //the second phase: iterative processing of the local state table
private:
    MaiterKernel<K, V, D>* maiter;                  //user-defined iteratekernel
    vector<pair<K, V> >* output;                    //the output buffer          
    vector<pair<int, V> >* output_mirrors;//partition,delta
public:
    
    void set_maiter(MaiterKernel<K, V, D>* inmaiter) {
        maiter = inmaiter;
    }

        void show_data(K key, V delta, V value, D data, int data_property, D mirrors) {
            LOG(INFO) << "key=" << key << ", value=" << value << ", delata=" << delta
                    << ", data_property=" << data_property;
             
            LOG(INFO) << "\n";
        }
     void dump(TypedGlobalTable<K, V, V, D>* a,int step){
        fstream File;                   //the output file containing the local state table infomatio
        string file = StringPrintf("%s/part-%d-%d", maiter->output.c_str(), current_shard(),step);  //the output path
        File.open(file.c_str(), ios::out);
        typename TypedGlobalTable<K, V, V, D>::Iterator *it = a->get_entirepass_iterator(current_shard());
        while(!it->done()) {
                bool cont = it->Next();
                if(!cont) break;
                File << it->position()<<"\t"<<it->key() << "\t"  << it->value2() << "\n";
        }
         //File << "\n";
        delete it;
        File.clear();
        File.close();
    }
        double start_time;
        double time1;
        double time2;
        double time3;
        double time4;
        double totaltime1;
        double totaltime2;
        double totaltime3;
        double totaltime4;
        double starttime=0;
    void run_iter(const K& k, V &v1, V &v2, D &v3,int& v4,D &v5,const K& master) {
   // void run_iter(TypedGlobalTable<K, V, V, D>::Iterator *it2){
//        int shard = current_shard();
//        V delta=it2->value1();
//        //maiter->iterkernel->process_delta_v(k,v1,v2,v3);//暂未修改
//        //bool is_master=(master==shard);
//        start_time=Now();
//        it2->updateF1F2(master==shard);//对master_vertex进行value的更新
//            // maiter->table->accumulateF2(k,v1,shard); //只对master_vertex进行value的更新  //perform v=v+delta_v    
//                              
//        time1= Now();
//        totaltime1 +=(time1-start_time);                                                                  // process delta_v before accumulate
//       
//        maiter->iterkernel->g_func(it2->key(),delta, it2->value2(), it2->value3(), it2->value4(),output);      //invoke api, perform g(delta_v) and send messages to out-neighbors
//        time2= Now();
//        totaltime2 +=(time2-time1);  
//        
//        //此函数会进行周期性的接受消息
//        //maiter->table->updateF1(k,maiter->iterkernel->default_v(),shard);        //perform delta_v=0, reset delta_v after delta_v has been spread out
//        time3= Now();
//        totaltime3 +=(time3-time2);  
//        
//        typename vector<pair<K, V> >::iterator iter;
//        for(iter = output->begin(); iter != output->end(); iter++) {        //send the buffered messages to remote state table
//                pair<K, V> kvpair = *iter;
//                maiter->table->accumulateF1(kvpair.first, kvpair.second,shard,shard,true);   //apply the output messages to remote state table
//        }
//        time4= Now();
//        totaltime4 +=(time4-time3);  
//
//        output->clear();                                                    //clear the output buffer
        
    }

    void run_loop(TypedGlobalTable<K, V, V, D>* a) {
        Timer timer;                        //for experiment, time recording
        //double totalF1 = 0;                 //the sum of delta_v, it should be smaller and smaller as iterations go on
        double totalF2 = 0;                 //the sum of v, it should be larger and larger as iterations go on
        long updates = 0;                   //for experiment, recording number of update operations
        output = new vector<pair<K, V> >;
        output_mirrors=new vector<pair<int, V> >;
        int step=0;
        double time1_=Now();
        double time2_;
        //the main loop for iterative update
       // VLOG(0)<<"worker:"<<current_shard()<<endl;
        while(true){
            time2_ =Now();
           // VLOG(0)<<"worker"<<current_shard()<<":第"<<step++<<"次迭代,时间="<<time2_-time1_<<endl;
            time1_=time2_;
            totaltime1=0;
            totaltime2 =0;
            totaltime3=0;
            totaltime4=0;
            starttime=0;
//      
            //get the iterator of the local state table
            start_time=Now();
            typename TypedGlobalTable<K, V, V, D>::Iterator *it2 = a->get_typed_iterator(current_shard(), false);
            if(it2 == NULL) {break;}//当terminate_=true时,it2=NULL.
            //starttime=Now()-start_time;  
            //VLOG(0)<<"worker:"<<current_shard()<<"******************8"<<endl;
            //should not use for(;!it->done();it->Next()), that will skip some entry
                while (!it2->done()) {
                    bool cont = it2->Next(); //if we have more in the state table, we continue
                    if (!cont) break;
                    //1）通信顶点
                    //if(it2->value4()==0)continue;//如果是通信副本顶点则过滤掉
                    totalF2 += it2->value2(); //for experiment, recording the sum of v
                    updates++; //for experiment, recording the number of updates
                    int master= it2->master();
                    int shard = current_shard();
                    V delta = it2->value1();
                    const K& k=it2->key();
                   // if(shard==0)VLOG(0)<<"0key:"<<k;
                    //maiter->iterkernel->process_delta_v(k,v1,v2,v3);//暂未修改
                   //第一步更新 value和delta
//                    start_time = Now();
                    //2）master顶点
                    it2->updateF1F2(master == shard,output_mirrors); //对value delta 
                    // maiter->table->accumulateF2(k,v1,shard); //只对master_vertex进行value的更新  //perform v=v+delta_v    
//                    time1 = Now();
//                    totaltime1 += (time1 - start_time); // process delta_v before accumulate
                    
                    //发送远程消息
                    if(master == shard){
                         //if(shard==0)VLOG(0)<<"key:"<<k;
                         typename vector<pair<int, V> >::iterator iter1;
                         for(iter1 = output_mirrors->begin(); iter1 != output_mirrors->end(); iter1++) { //send the buffered messages to remote state table
                            pair<int, V> kvpair = *iter1;
                            maiter->table->accumulateDelta(k, kvpair.second, kvpair.first); //apply the output messages to remote state table
                           //if(shard==0)VLOG(0)<<"key="<<k<<" value="<<kvpair.second<<"  to part"<<kvpair.first<<endl;
                         }  //k顶点  second值 first机器号
                         output_mirrors->clear();
                    }
                    
                    maiter->iterkernel->g_func(k, delta, it2->value2(), it2->value3(), it2->value4(), output); //invoke api, perform g(delta_v) and send messages to out-neighbors
//                    time2 = Now();
//                    totaltime2 += (time2 - time1);   
//                    time3 = Now();
//                    totaltime3 += (time3 - time2);
                    //发送本地消息
                    typename vector<pair<K, V> >::iterator iter;
                    for (iter = output->begin(); iter != output->end(); iter++) { //send the buffered messages to remote state table
                        pair<K, V> kvpair = *iter;
                        maiter->table->accumulateF1(kvpair.first, kvpair.second, shard, shard, true); //apply the output messages to remote state table
                    }
//                    time4 = Now();
//                    totaltime4 += (time4 - time3);
                    output->clear();
                }
                delete it2;                        //delete the table iterator
            //dump(a,step);
           // if(current_shard()==0){
//            LOG(INFO) <<"worker"<<current_shard()<<"totaltime1="<<totaltime1<<endl;
//            LOG(INFO) <<"worker"<<current_shard()<<"totaltime2="<<totaltime2<<endl;
//            LOG(INFO) <<"worker"<<current_shard()<<"totaltime3="<<totaltime3<<endl; 
//            LOG(INFO) <<"worker"<<current_shard()<<"totaltime4="<<totaltime4<<endl; 
//            LOG(INFO) <<"worker"<<current_shard()<<"starttime="<<starttime<<endl; 
           // }
        }
    }

    void map() {
        VLOG(0) << "start performing iterative update";
        //VLOG(0) << "work="<<maiter->table->helper()->id()<<"  shard="<<current_shard()<<endl;
        run_loop(maiter->table);
    }
};

template <class K, class V, class D>
class MaiterKernel3 : public DSMKernel {        //the third phase: dumping the result, write the in-memory table to disk
private:
    MaiterKernel<K, V, D>* maiter;              //user-defined iteratekernel
public:
    void set_maiter(MaiterKernel<K, V, D>* inmaiter) {
        maiter = inmaiter;
    }
        
    void dump(TypedGlobalTable<K, V, V, D>* a){
        double totalF1 = 0;             //the sum of delta_v, it should be smaller enough when iteration converges  
        double totalF2 = 0;             //the sum of v, it should be larger enough when iteration converges
        fstream File;                   //the output file containing the local state table infomation

        string file = StringPrintf("%s/part-%d", maiter->output.c_str(), current_shard());  //the output path
        File.open(file.c_str(), ios::out);
        int shard_ =current_shard();
        //get the iterator of the local state table
        typename TypedGlobalTable<K, V, V, D>::Iterator *it = a->get_entirepass_iterator(shard_);
         //后续我会在创建一个iterator来遍历所有的master顶点
        //一会写###################??
        while(!it->done()) {
                bool cont = it->Next();
                if(!cont) break;
                
                totalF1 += it->value1();
                totalF2 += it->value2();
                //输出1
                
//                if(it->master()==shard_){//只将master顶点
//                    File << "key=" << it->key() << ", value="  << it->value2() << ", delata=" <<it->value1()
//                        << ", priority=" <<it->priority()<< ", length=" <<it->value4()<<", data=";
//                    for(int i=0;i<it->value3().size();++i){
//                        File << it->value3()[i]<<" ";
//                    }
//                    File << ", mirrors=";
//                    for(int i=0;i<it->value5().size();++i){
//                        File << it->value5()[i]<<" ";
//                    }
//                    File<<"master="<<it->master();
//                    File <<"\n";
//                }
                //输出2
                
                if(it->master()==current_shard()){//只将master顶点
                      File << it->key() << "\t"  << it->value2() << "\n";
                }
//                  if(it->master()){//只将master顶点
//                      VLOG(0) << it->key() << "\t"  << it->value2() << "\n";
//                  }
        }
        delete it;
        File.close();

        cout << "total F1 : " << totalF1 << endl;
        cout << "total F2 : " << totalF2 << endl;
    }

    void run() {
        VLOG(0) << "dumping result";
        dump(maiter->table);
    }
};


template <class K, class V, class D>
class MaiterKernel{
    
public:
        
    int64_t num_nodes;
    double schedule_portion;
    ConfigData conf;
    string output;
    Sharder<K> *sharder;
    IterateKernel<K, V, D> *iterkernel;
    TermChecker<K, V> *termchecker;

    TypedGlobalTable<K, V, V, D> *table;

    
    MaiterKernel() { Reset(); }

    MaiterKernel(ConfigData& inconf, int64_t nodes, double portion, string outdir,
                    Sharder<K>* insharder,                  //the user-defined partitioner
                    IterateKernel<K, V, D>* initerkernel,   //the user-defined iterate kernel
                    TermChecker<K, V>* intermchecker) {     //the user-defined terminate checker
        Reset();
        
        conf = inconf;                  //configuration
        num_nodes = nodes;              //local state table size 近似
        schedule_portion = portion;     //priority scheduling, scheduled portion
        output = outdir;                //output dir
        sharder = insharder;            //the user-defined partitioner
        iterkernel = initerkernel;      //the user-defined iterate kernel
        termchecker = intermchecker;    //the user-defined terminate checker
    }
    
    ~MaiterKernel(){}
    void Reset() {
        num_nodes = 0;
        schedule_portion = 1;
        output = "result";
        sharder = NULL;
        iterkernel = NULL;
        termchecker = NULL;
    }
public:
    int registerMaiter() {
	VLOG(0) << "shards " << conf.num_workers();
        //LOG(INFO) << "conf.num_workers()="<<conf.num_workers();
	table = CreateTable<K, V, V, D >(0, conf.num_workers(), schedule_portion,
                                        sharder, iterkernel, termchecker);
            
        //initialize table job
        KernelRegistrationHelper<MaiterKernel1<K, V, D>, K, V, D>("MaiterKernel1", this);
        MethodRegistrationHelper<MaiterKernel1<K, V, D>, K, V, D>("MaiterKernel1", "run", &MaiterKernel1<K, V, D>::run, this);

        //iterative update job
        if(iterkernel != NULL){
            KernelRegistrationHelper<MaiterKernel2<K, V, D>, K, V, D>("MaiterKernel2", this);
            MethodRegistrationHelper<MaiterKernel2<K, V, D>, K, V, D>("MaiterKernel2", "map", &MaiterKernel2<K, V, D>::map, this);
        }

        //dumping result to disk job
        if(termchecker != NULL){
            KernelRegistrationHelper<MaiterKernel3<K, V, D>, K, V, D>("MaiterKernel3", this);
            MethodRegistrationHelper<MaiterKernel3<K, V, D>, K, V, D>("MaiterKernel3", "run", &MaiterKernel3<K, V, D>::run, this);
        }
     
	return 0;
    }
};


class RunnerRegistry {
public:
  typedef int (*KernelRunner)(ConfigData&);
  typedef map<string, KernelRunner> Map;

  KernelRunner runner(const string& name) { return m_[name]; }
  Map& runners() { return m_; }

  static RunnerRegistry* Get();
private:
  RunnerRegistry() {}
  Map m_;
};

struct RunnerRegistrationHelper {
  RunnerRegistrationHelper(RunnerRegistry::KernelRunner k, const char* name) {
    RunnerRegistry::Get()->runners().insert(make_pair(name, k));
  }
};

#define REGISTER_KERNEL(klass)\
  static KernelRegistrationHelper<klass> k_helper_ ## klass(#klass);

#define REGISTER_METHOD(klass, method)\
  static MethodRegistrationHelper<klass> m_helper_ ## klass ## _ ## method(#klass, #method, &klass::method);

#define REGISTER_RUNNER(r)\
  static RunnerRegistrationHelper r_helper_ ## r ## _(&r, #r);

}
#endif /* KERNELREGISTRY_H_ */
