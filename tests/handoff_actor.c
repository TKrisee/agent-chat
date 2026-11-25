/* Private distinct configured-client/resume-CLI fixture. No native protocol.
 * Keeps one actual connection, reconnects after closure, exits on stdin EOF. */
#include <arpa/inet.h>
#include <poll.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
int main(int argc,char **argv) {
    const char *endpoint=NULL;
    for(int i=1;i+1<argc;i++)
        if(!strcmp(argv[i],"--remote")||!strcmp(argv[i],"--codex-server")) endpoint=argv[i+1];
    if(!endpoint||strncmp(endpoint,"ws://127.0.0.1:",15))return 2;
    struct sockaddr_in address={.sin_family=AF_INET,.sin_port=htons(atoi(endpoint+15))};
    address.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    alarm(300);
    int fd=-1;
    for(;;) {
        struct pollfd input={.fd=0,.events=POLLIN|POLLHUP};
        if(poll(&input,1,0)>0) { char b; if(read(0,&b,1)<=0) return 0; }
        if(fd<0) {
            fd=socket(AF_INET,SOCK_STREAM,0);
            if(connect(fd,(struct sockaddr *)&address,sizeof(address))) { close(fd);fd=-1;usleep(20000);continue; }
        }
        struct pollfd items[2]={{.fd=0,.events=POLLIN|POLLHUP},{.fd=fd,.events=POLLIN|POLLHUP}};
        if(poll(items,2,20)>0) {
            if(items[0].revents) {char b;if(read(0,&b,1)<=0){close(fd);return 0;}}
            if(items[1].revents) {char b;if(read(fd,&b,1)<=0){close(fd);fd=-1;}}
        }
    }
}
