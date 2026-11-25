#include <Security/Security.h>
#include <CoreFoundation/CoreFoundation.h>
#include <libproc.h>
#include <sys/proc_info.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static void status(const char *name, OSStatus value) {
    printf("%s\t%d",name,(int)value);
    CFStringRef msg=SecCopyErrorMessageString(value,NULL);
    if(msg){char buf[1024];if(CFStringGetCString(msg,buf,sizeof(buf),kCFStringEncodingUTF8))printf("\t%s",buf);CFRelease(msg);} puts("");
}
static int identity(const char *label,int pid,struct proc_bsdinfo *info){
    int n=proc_pidinfo(pid,PROC_PIDTBSDINFO,0,info,sizeof(*info));
    if(n!=sizeof(*info)){printf("%s\tmissing\t%d\n",label,n);return 0;}
    printf("%s\tpid=%u\tppid=%u\tpgid=%u\tuid=%u\tbirth=%llu.%06llu\n",label,info->pbi_pid,info->pbi_ppid,info->pbi_pgid,info->pbi_uid,info->pbi_start_tvsec,info->pbi_start_tvusec);return 1;
}
static CFDataRef hash(SecStaticCodeRef code,const char *label){
    CFDictionaryRef info=NULL;OSStatus s=SecCodeCopySigningInformation(code,kSecCSSigningInformation,&info);status(label,s);
    if(s!=errSecSuccess)return NULL;
    CFDataRef value=CFDictionaryGetValue(info,kSecCodeInfoUnique); if(value&&CFGetTypeID(value)==CFDataGetTypeID()){
      CFRetain(value);printf("%s_cdhash\t",label);const UInt8 *p=CFDataGetBytePtr(value);for(CFIndex i=0;i<CFDataGetLength(value);i++)printf("%02x",p[i]);puts("");
    } else value=NULL;
    CFRelease(info);return value;
}
int main(int argc,char **argv){
    if(argc!=5)return 2;
    int pid=atoi(argv[1]); if(pid<=1)return 2;struct proc_bsdinfo before={0},after={0};
    if(!identity("before",pid,&before))return 3;
    if(before.pbi_start_tvsec!=strtoull(argv[2],NULL,10)||before.pbi_start_tvusec!=strtoull(argv[3],NULL,10)){puts("selected_pid_birth_changed; stopped");return 4;}
    CFNumberRef number=CFNumberCreate(NULL,kCFNumberIntType,&pid);const void *keys[]={kSecGuestAttributePid},*values[]={number};
    CFDictionaryRef attrs=CFDictionaryCreate(NULL,keys,values,1,&kCFTypeDictionaryKeyCallBacks,&kCFTypeDictionaryValueCallBacks);
    SecCodeRef guest=NULL;OSStatus s=SecCodeCopyGuestWithAttributes(NULL,attrs,kSecCSDefaultFlags,&guest);status("dynamic_guest",s);
    CFDataRef liveHash=NULL,diskHash=NULL;OSStatus valid=-1;
    if(s==errSecSuccess){
      valid=SecCodeCheckValidity(guest,kSecCSNoNetworkAccess,NULL);status("dynamic_validity",valid);
      SecStaticCodeRef st=NULL;s=SecCodeCopyStaticCode(guest,kSecCSDefaultFlags,&st);status("dynamic_static_projection",s);
      if(s==errSecSuccess){liveHash=hash(st,"dynamic_projection");CFRelease(st);}
      CFRelease(guest);
    }
    CFURLRef url=CFURLCreateFromFileSystemRepresentation(NULL,(const UInt8*)argv[4],strlen(argv[4]),false);
    SecStaticCodeRef disk=NULL;s=SecStaticCodeCreateWithPath(url,kSecCSDefaultFlags,&disk);status("disk_code",s);OSStatus diskValid=-1;
    if(s==errSecSuccess){diskValid=SecStaticCodeCheckValidity(disk,kSecCSNoNetworkAccess,NULL);status("disk_validity",diskValid);diskHash=hash(disk,"disk");CFRelease(disk);}
    int stable=identity("after",pid,&after)&&before.pbi_pid==after.pbi_pid&&before.pbi_ppid==after.pbi_ppid&&before.pbi_pgid==after.pbi_pgid&&before.pbi_uid==after.pbi_uid&&before.pbi_start_tvsec==after.pbi_start_tvsec&&before.pbi_start_tvusec==after.pbi_start_tvusec;
    int same=liveHash&&diskHash&&CFEqual(liveHash,diskHash);
    printf("projection_hash_equal\t%s\n",same?"true":"false");
    printf("validated_dynamic_disk_association\t%s\n",stable&&valid==0&&diskValid==0&&same?"true":"UNPROVEN");
    if(liveHash)CFRelease(liveHash);if(diskHash)CFRelease(diskHash);CFRelease(url);CFRelease(attrs);CFRelease(number);return 0;
}
