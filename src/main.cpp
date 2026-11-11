#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <sstream>
#include <condition_variable>
using namespace std;
using Clock=chrono::steady_clock;
struct Backend {string host;int port;atomic<bool> healthy{false};atomic<int> active{0};atomic<uint64_t> served{0},errors{0};Backend(string h,int p):host(move(h)),port(p){};};
vector<shared_ptr<Backend>> servers;
atomic<bool> running{true};atomic<uint64_t> accepted{0},completed{0},failed{0},rejected{0},bytes_in{0},bytes_out{0};
atomic<int> workers{0};atomic<uint64_t> rr{0};mutex io_mu,rate_mu,worker_mu;condition_variable worker_cv;
string algorithm="round-robin",logfile="routex.csv";int max_clients=256,rate_limit=60,connect_timeout_ms=1500,idle_timeout_ms=30000,health_ms=2000,admin_port=0;
struct Bucket {Clock::time_point start;int count=0;};unordered_map<string,Bucket> rates;
void info(const string&s){lock_guard<mutex> g(io_mu);cerr<<s<<'\n';}
void record(const string&ip,const string&target,const string&status,uint64_t up,uint64_t down){lock_guard<mutex> g(io_mu);ofstream f(logfile,ios::app);f<<chrono::duration_cast<chrono::seconds>(chrono::system_clock::now().time_since_epoch()).count()<<','<<ip<<','<<target<<','<<status<<','<<up<<','<<down<<'\n';}
int nonblock(int fd){int flags=fcntl(fd,F_GETFL,0);return flags<0?-1:fcntl(fd,F_SETFL,flags|O_NONBLOCK);}
int dial(const Backend& b,int timeout){int fd=socket(AF_INET,SOCK_STREAM,0);if(fd<0)return -1;nonblock(fd);sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(b.port);if(inet_pton(AF_INET,b.host.c_str(),&a.sin_addr)!=1){close(fd);return -1;}int rc=connect(fd,(sockaddr*)&a,sizeof(a));if(rc<0&&errno!=EINPROGRESS){close(fd);return -1;}if(rc<0){pollfd p{fd,POLLOUT,0};if(poll(&p,1,timeout)<=0){close(fd);return -1;}int err=0;socklen_t len=sizeof(err);if(getsockopt(fd,SOL_SOCKET,SO_ERROR,&err,&len)<0||err){close(fd);return -1;}}return fd;}
int listener(int port){int fd=socket(AF_INET,SOCK_STREAM,0);if(fd<0)return -1;int yes=1;setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(yes));nonblock(fd);sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(port);a.sin_addr.s_addr=htonl(INADDR_LOOPBACK);if(bind(fd,(sockaddr*)&a,sizeof(a))<0||listen(fd,256)<0){close(fd);return -1;}return fd;}
void health_loop(){while(running){for(auto& b:servers){if(!running)break;int fd=dial(*b,connect_timeout_ms);bool ok=fd>=0;if(ok)close(fd);bool prev=b->healthy.exchange(ok);if(prev!=ok)info("[HEALTH] "+b->host+":"+to_string(b->port)+(ok?" UP":" DOWN"));}for(int t=0;t<health_ms/50&&running;t++)this_thread::sleep_for(chrono::milliseconds(50));}}
shared_ptr<Backend> choose(){vector<shared_ptr<Backend>> live;for(auto& b:servers)if(b->healthy)live.push_back(b);if(live.empty())return {};if(algorithm=="least-connections")return *min_element(live.begin(),live.end(),[](auto&a,auto&b){return a->active.load()<b->active.load();});return live[rr++%live.size()];}
bool permitted(const string& ip){lock_guard<mutex> g(rate_mu);auto now=Clock::now();auto& b=rates[ip];if(now-b.start>=chrono::seconds(60)){b.start=now;b.count=0;}return ++b.count<=rate_limit;}
// Full-duplex, nonblocking TCP relay. Buffers enforce backpressure and support half-closes.
struct Pipe {string pending;bool eof=false,shutdown=false;uint64_t transferred=0;};
bool relay(int a,int b,uint64_t& to_backend,uint64_t& to_client){Pipe ab,ba;auto last=Clock::now();constexpr size_t CAP=65536;bool ok=true;
 while(running){pollfd fds[2]={{a,0,0},{b,0,0}};
 if(!ab.eof&&ab.pending.size()<CAP)fds[0].events|=POLLIN;
 if(!ba.eof&&ba.pending.size()<CAP)fds[1].events|=POLLIN;
 if(!ba.pending.empty())fds[0].events|=POLLOUT;
 if(!ab.pending.empty())fds[1].events|=POLLOUT;
 int rc=poll(fds,2,250);if(rc<0){if(errno==EINTR)continue;ok=false;break;}
 if(Clock::now()-last>chrono::milliseconds(idle_timeout_ms)){ok=false;break;}
 if(rc>0){
 auto read_side=[&](int fd,short events,Pipe& p){if(!(events&POLLIN))return;char buf[16384];ssize_t n=recv(fd,buf,min(sizeof(buf),CAP-p.pending.size()),0);if(n>0){p.pending.append(buf,n);last=Clock::now();}else if(n==0)p.eof=true;else if(errno!=EAGAIN&&errno!=EWOULDBLOCK&&errno!=EINTR){p.eof=true;ok=false;}};
 read_side(a,fds[0].revents,ab);read_side(b,fds[1].revents,ba);
 auto write_side=[&](int fd,short events,Pipe& p){if(!(events&POLLOUT)||p.pending.empty())return;ssize_t n=send(fd,p.pending.data(),p.pending.size(),0);if(n>0){p.pending.erase(0,n);p.transferred+=n;last=Clock::now();}else if(n<0&&errno!=EAGAIN&&errno!=EWOULDBLOCK&&errno!=EINTR){ok=false;p.pending.clear();p.eof=true;}};
 write_side(b,fds[1].revents,ab);write_side(a,fds[0].revents,ba);
 if((fds[0].revents|fds[1].revents)&(POLLERR|POLLNVAL)){ok=false;break;}
 if(fds[0].revents&POLLHUP)ab.eof=true;
 if(fds[1].revents&POLLHUP)ba.eof=true;
 }
 if(ab.eof&&ab.pending.empty()&&!ab.shutdown){shutdown(b,SHUT_WR);ab.shutdown=true;}
 if(ba.eof&&ba.pending.empty()&&!ba.shutdown){shutdown(a,SHUT_WR);ba.shutdown=true;}
 if(ab.shutdown&&ba.shutdown)break;
 }
 to_backend=ab.transferred;to_client=ba.transferred;return ok;
}
void handle(int client,string ip){string target="none",status="failed";uint64_t up=0,down=0;shared_ptr<Backend> selected;int upstream=-1;
 for(size_t attempt=0;attempt<servers.size();attempt++){auto b=choose();if(!b)break;int fd=dial(*b,connect_timeout_ms);if(fd>=0){selected=b;upstream=fd;break;}b->errors++;b->healthy=false;}
 if(selected){target=selected->host+":"+to_string(selected->port);selected->active++;bool ok=relay(client,upstream,up,down);selected->active--;selected->served++;bytes_in+=up;bytes_out+=down;if(ok){completed++;status="ok";}else{failed++;status="relay_error";}}else{failed++;const string msg="HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";send(client,msg.data(),msg.size(),0);}
 if(upstream>=0)close(upstream);
 close(client);record(ip,target,status,up,down);workers--;worker_cv.notify_all();}
string metrics(){ostringstream s;s<<"{\"accepted\":"<<accepted<<",\"completed\":"<<completed<<",\"failed\":"<<failed<<",\"rejected\":"<<rejected<<",\"active\":"<<workers<<",\"bytes_to_backends\":"<<bytes_in<<",\"bytes_to_clients\":"<<bytes_out<<",\"backends\":[";for(size_t i=0;i<servers.size();i++){auto& b=*servers[i];if(i)s<<',';s<<"{\"address\":\""<<b.host<<':'<<b.port<<"\",\"healthy\":"<<(b.healthy?"true":"false")<<",\"active\":"<<b.active<<",\"served\":"<<b.served<<",\"errors\":"<<b.errors<<'}';}s<<"]}";return s.str();}
void admin_loop(){int fd=listener(admin_port);if(fd<0){info("[ERROR] admin bind failed");return;}while(running){pollfd p{fd,POLLIN,0};if(poll(&p,1,200)<=0)continue;int c=accept(fd,nullptr,nullptr);if(c<0)continue;timeval tv{1,0};setsockopt(c,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));char request[1024]{};ssize_t n=recv(c,request,sizeof(request)-1,0);string req=n>0?string(request,n):"";bool valid=req.rfind("GET /metrics ",0)==0||req.rfind("GET /health ",0)==0;string body=valid?(req.rfind("GET /health ",0)==0?"OK\n":metrics()):"Not found\n";string reply="HTTP/1.1 "+string(valid?"200 OK":"404 Not Found")+"\r\nContent-Type: "+string(valid?"application/json":"text/plain")+"\r\nContent-Length: "+to_string(body.size())+"\r\nConnection: close\r\n\r\n"+body;send(c,reply.data(),reply.size(),0);close(c);}close(fd);}
void signal_handler(int){running=false;}
int number(const string&s,int low,int high){try{size_t pos;int n=stoi(s,&pos);if(pos==s.size()&&n>=low&&n<=high)return n;}catch(...){}throw runtime_error("invalid number: "+s);}
int main(int argc,char**argv){try{
 if(argc<4){cerr<<"Usage: routex <listen_port> <ip:port> <ip:port> ... [--algorithm round-robin|least-connections] [--admin-port PORT] [--max-clients N] [--rate-limit N] [--log FILE] [--idle-ms N] [--health-ms N]\n";return 1;}
 int port=number(argv[1],1,65535);int i=2;for(;i<argc&&string(argv[i]).rfind("--",0)!=0;i++){string arg=argv[i];auto p=arg.rfind(':');if(p==string::npos)throw runtime_error("invalid backend "+arg);string host=arg.substr(0,p);in_addr addr{};if(inet_pton(AF_INET,host.c_str(),&addr)!=1)throw runtime_error("IPv4 required: "+host);servers.push_back(make_shared<Backend>(host,number(arg.substr(p+1),1,65535)));}if(servers.empty())throw runtime_error("provide backends");
 for(;i<argc;i++){string opt=argv[i];if(i+1>=argc)throw runtime_error("missing value for "+opt);string val=argv[++i];if(opt=="--algorithm"){if(val!="round-robin"&&val!="least-connections")throw runtime_error("invalid algorithm");algorithm=val;}else if(opt=="--admin-port")admin_port=number(val,1,65535);else if(opt=="--max-clients")max_clients=number(val,1,100000);else if(opt=="--rate-limit")rate_limit=number(val,1,100000);else if(opt=="--idle-ms")idle_timeout_ms=number(val,100,3600000);else if(opt=="--health-ms")health_ms=number(val,100,60000);else if(opt=="--log")logfile=val;else throw runtime_error("unknown option "+opt);}
 signal(SIGPIPE,SIG_IGN);signal(SIGINT,signal_handler);signal(SIGTERM,signal_handler);
 {ofstream f(logfile,ios::app);if(!f)throw runtime_error("cannot open log file");if(f.tellp()==0)f<<"timestamp,client,backend,status,bytes_to_backend,bytes_to_client\n";}
 int fd=listener(port);if(fd<0)throw runtime_error("cannot listen on 127.0.0.1:"+to_string(port));thread health(health_loop);thread admin;if(admin_port)admin=thread(admin_loop);info("[START] RouteX 127.0.0.1:"+to_string(port)+" algorithm="+algorithm);
 while(running){pollfd p{fd,POLLIN,0};if(poll(&p,1,200)<=0)continue;sockaddr_in addr{};socklen_t len=sizeof(addr);int c=accept(fd,(sockaddr*)&addr,&len);if(c<0)continue;char ipbuf[INET_ADDRSTRLEN]{};inet_ntop(AF_INET,&addr.sin_addr,ipbuf,sizeof(ipbuf));string ip=ipbuf;accepted++;
 if(workers>=max_clients||!permitted(ip)){rejected++;close(c);continue;}nonblock(c);workers++;try{thread(handle,c,ip).detach();}catch(...){workers--;close(c);failed++;}}
 close(fd);health.join();if(admin.joinable())admin.join();{unique_lock<mutex> lk(worker_mu);worker_cv.wait(lk,[]{return workers.load()==0;});}info("[STOP] "+metrics());return 0;
 }catch(const exception&e){cerr<<"Error: "<<e.what()<<'\n';return 1;}}
