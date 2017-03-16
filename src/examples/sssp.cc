#include "client/client.h"
#define S "0"     //令源节点是id=0

using namespace dsm;

//DECLARE_string(graph_dir);
DECLARE_string(result_dir);
DECLARE_int64(num_nodes);
DECLARE_double(portion);


struct Ssspiterate : public IterateKernel<string, double, vector<vector<double> > > {
    double imax;
    Ssspiterate() {
        imax = std::numeric_limits<double>::max();
    }//初始化列表函数
    //输入格式必须是”4	5 60	3 20“这种形式。
    void read_data(string& line, string& k, vector<vector<double> >& data){
        string linestr(line);
        linestr+="\t";
        int pos= linestr.find("\t");
        if(pos==string::npos)return; 
        string source = linestr.substr(0,pos);
         //VLOG(0)<<"FLAGS_termcheck_threshold: "<<FLAGS_termcheck_threshold;
        k=source;
        linestr = linestr.substr(pos+1);
        
        while(linestr.find("\t")!=-1){
            pos=linestr.find(" ");
            double id = boost::lexical_cast<double>(linestr.substr(0,pos));
            linestr = linestr.substr(pos+1);
            pos = linestr.find("\t");
            double value = boost::lexical_cast<double>(linestr.substr(0,pos));
            linestr = linestr.substr(pos+1);
            vector<double> vectortmp;
            vectortmp.push_back(id);
            vectortmp.push_back(value);
            data.push_back(vectortmp);
        }
        //格式分析
        VLOG(0)<<endl<<"source="<<source<<" ";
        for(int i=0; i< data.size(); i++){
                string key=boost::lexical_cast<string>(data[i][0]);
                double outv=data[i][1];
                VLOG(0)<<i<<":"<<key<<","<<outv;
            }
        VLOG(0)<<endl;
    }
    void init_c(const string& k, double& delta,vector<vector<double> >& data){
           if(k==S)
               delta=0;
           else
               delta=imax;     
    } 
    void init_v(const string& k,double& v,vector<vector<double> >& data){
            v=imax;
    }
    /*void process_delta_v(const string& k, double& delta, double& value, vector<vector<double> >& data){
            if(delta==imax) return;
            delta=delta-1;
    }*/
    void priority(double& pri, const double& value, const double& delta){
            pri = value-min(value,delta);
    }
    void accumulate(double& a, const double& b){
            a=min(a,b);
    }
    void offset(float& a, const float& b){
            //空操作  //应该想办法排除掉如果a=b,则不应该在传递出去
    }
    void g_func(const string& k, const double& delta,const double&value, const vector<vector<double> >& data, vector<pair<string, double> >* output){
            for(int i=0; i< data.size(); i++){
                string key=boost::lexical_cast<string>(data[i][0]);
                double outv=data[i][1]+delta;
                output->push_back(make_pair(key,outv));
            }
    }
   
    const double& default_v() const {
            return imax;
    }   
 };   
 
  static int Sssp(ConfigData& conf) {
     MaiterKernel<string, double, vector<vector<double> > >* kernel = new MaiterKernel<string, double, vector<vector<double> > >(
                                        conf, FLAGS_num_nodes, FLAGS_portion, FLAGS_result_dir,
                                        new Sharding::Mod_str,
                                        new Ssspiterate,
                                        new TermCheckers<string, double>::Diff);
    
    
    kernel->registerMaiter();

    if (!StartWorker(conf)) {
        Master m(conf);
        m.run_maiter(kernel);
    }
    
    delete kernel;
    return 0;
}

REGISTER_RUNNER(Sssp);


//重写终止函数 
 // template <string K, double V>
 /* struct Summ:public TermCheckers<string,double> {
    double last;
    double curr;
    
    Summ(){
        last = -std::numeric_limits<double>::max();//将最小值赋值给last
        curr = 0;
    }

    double set_curr(){
        return curr;
    }
    
    double estimate_prog(LocalTableIterator<string, double>* statetable){
        double partial_curr = 0;
        double defaultv = statetable->defaultV();//获得逻辑上的 零值 上边的类定义的
        while(!statetable->done()){
            bool cont = statetable->Next();
            if(!cont) break;
            //statetable->Next();
            //cout << statetable->key() << "\t" << statetable->value2() << endl;
            if(statetable->value2() != defaultv){
                partial_curr += static_cast<double>(statetable->value2());
            }
        }
        return partial_curr;
    }
    
    bool terminate(vector<double> local_reports){
        curr = 0;
        vector<double>::iterator it;
        for(it=local_reports.begin(); it!=local_reports.end(); it++){
                curr += *it;
        }
        
        VLOG(0) << "terminate check : last progress " << last << " current progress " << curr << " difference " << abs(curr - last);
        if(abs(curr - last) <= FLAGS_termcheck_threshold){//？？flags
            return true;
        }else{
            last = curr;
            return false;
        }
    }
  };*/
