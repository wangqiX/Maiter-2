#include "client/client.h"


using namespace dsm;

//DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);

struct PagerankIterateKernel : public IterateKernel<int, float, vector<int> > {
    float zero;

    PagerankIterateKernel() : zero(0){}// zero(0)？？？
    //重写数据读取函数
    vector<string> split(string str,string pattern,bool filter)//filter用于过滤多个分隔符  
    {
        string::size_type pos;
        vector<string> result;
        str += pattern; //扩展字符串以方便操作  
        int size = str.size();
        for (int i = 0; i < size; ++i) {
            pos = str.find(pattern, i);
            if (pos < size) {
                string s = str.substr(i, pos - i);
                if (filter) {
                    if (s.length() == 0)continue;
                }
                result.push_back(s);
                i = pos + pattern.size() - 1;
            }
        }
        return result;
    } 
    //input:1;0;1,2,3,;5;2,9,key;master;mirrors;length;link
    
    
    void read_data(string& line, int& k, vector<int>& data,int& data_property,map<int,float>& mirrors,int& master){
        
        string linestr(line);
        vector<string> input_vec =split(linestr,";",false);
        if(input_vec.size()!=5)VLOG(0)<<"???????????????????"<<input_vec.size()<<endl<<endl;
        //获取key
        if(input_vec.size() == 0) return;
        k = boost::lexical_cast<int>(input_vec[0]);
        master=boost::lexical_cast<int>(input_vec[1]);
        //if(415711683==k)VLOG(0)<<"1k"<<k<<" line_"<<linestr<<endl;
        //获取mirrors
        if(input_vec[2].size()>0) {
            linestr = input_vec[2];
            vector<string> mirrors_vec = split(linestr, ",", true);
            for(int i=0;i<mirrors_vec.size();++i){
                mirrors.insert(make_pair<int,float>(boost::lexical_cast<int>(mirrors_vec[i]),0));
            }
        }
        //获取data_property 和 data
        if(input_vec[3].size()>0) {
            data_property =boost::lexical_cast<int>(input_vec[3]);
        }
        else{
            data_property=0;
        }
        
       //data
        if(input_vec[4].size()>0) {
            linestr = input_vec[4];
            vector<string> link_vec = split(linestr, ",", true);
            for(int i=0;i<link_vec.size();++i){
                data.push_back(boost::lexical_cast<int>(link_vec[i]));
            }
        }
         //if(415711683==k)VLOG(0)<<"data"<<data.size()<<" mirrors"<<mirrors.size()<<" master"<<master<<endl;
    }
    
    //2.1重写初始化函数，初始化V0和^V0（）
    void init_c(const int& k, float& delta,vector<int>& data,int data_property){
        if(data_property)
           delta = 0.2;
        else
            delta = 0;
    }
//
//    void init_sbuffer(const int& k, float& SBuffer){//缓存区初始化
//        SBuffer=0;
//    }
    
    void init_v(const int& k,float& v,vector<int>& data,int& data_property){
        v=0;  
    }
    //2.2重写累积函数，即指明运算符是+
    void accumulate(float& a, const float& b){
            a = a + b;
    }
   //抵消操作，即指明运算符是-
    void offset(float& a, const float& b){
            a = a - b;//应该想办法排除掉如果a=b,则不应该在传递出去
    }
    //2.3重写delta传递函数，即指明当前节点根据当前的delta，计算出对它指出节点的贡献值
    void g_func(const int& k,const float& delta, const float&value, const vector<int>& data, int& data_property, vector<pair<int, float> >* output){
            int size = data_property;
            float outv = delta * 0.8 / size;
            for(vector<int>::const_iterator it=data.begin(); it!=data.end(); it++){
                    int target = *it;
                    output->push_back(make_pair(target, outv));
            }
    }
   
    //3、重写优先值估计函数，即pri的值是多少（然后系统根据pri选择出优先队列，pri大的优先）
    void priority(float& pri, const float& value, const float& delta){
            pri = delta;
    }

    const float& default_v() const {
            return zero;
    }
};
//FLAGS_num_nodes  在Maiter+中FLAGS_num_nodes是对应每个worker上buckets_的大小,要事partx的值远远小于该值
//配置，并创建Master类，master类调用run函数执行程序
static int Pagerank(ConfigData& conf) {
    MaiterKernel<int, float, vector<int> >* kernel = new MaiterKernel<int, float, vector<int> >(
                                        conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
                                        new Sharding::Mod,
                                        new PagerankIterateKernel,
                                        new TermCheckers<int, float>::Diff);

    kernel->registerMaiter();
    //LOG(INFO)<<"ppppppppppppppppppp"<<FLAGS_termcheck_threshold<<"\t";
    if (!StartWorker(conf)) {
        Master m(conf);
        m.run_maiter(kernel);
    }
    
    delete kernel;
    return 0;
}

REGISTER_RUNNER(Pagerank);
