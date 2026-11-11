#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
int main(int argc,char**argv){if(argc<3){std::cerr<<"Usage: demo_backend <port> <name>\n";return 1;}int port=std::atoi(argv[1]);std::string name=argv[2];int fd=socket(AF_INET,SOCK_STREAM,0);int yes=1;setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(yes));sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(port);a.sin_addr.s_addr=htonl(INADDR_LOOPBACK);if(bind(fd,(sockaddr*)&a,sizeof(a))<0||listen(fd,32)<0){perror("backend");return 1;}std::cout<<name<<" listening on "<<port<<std::endl;while(true){int c=accept(fd,nullptr,nullptr);if(c<0)continue;char buf[4096];ssize_t n=recv(c,buf,sizeof(buf),0);if(n>0){std::string body="Hello from "+name+"\n";std::string reply="HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: "+std::to_string(body.size())+"\r\nConnection: close\r\n\r\n"+body;send(c,reply.data(),reply.size(),0);}close(c);}}
