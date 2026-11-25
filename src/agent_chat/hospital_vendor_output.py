"""Physical output-parent checks for the fixed Hospital vendor executor."""
import os
import stat

class Refused(ValueError): pass

def _open_absolute_directory(path):
    if not isinstance(path,str) or not path.startswith('/') or path=='/' or '\x00' in path or '\\' in path:
        raise Refused('directory_invalid')
    parts=path[1:].split('/')
    if any(part in ('','.','..') for part in parts): raise Refused('directory_invalid')
    flags=os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW
    fd=os.open('/',flags)
    try:
        for part in parts:
            nxt=os.open(part,flags,dir_fd=fd)
            os.close(fd); fd=nxt
        return fd
    except BaseException:
        os.close(fd); raise

def external_output_parent(project_root, output):
    """Return an owned parent FD outside project; caller must close it.

    This check is not task authority; a future fixed executor must revalidate a
    real server-issued binding around every read and before committing output.
    """
    if not isinstance(output,str) or not output.startswith('/') or '\x00' in output or '\\' in output:
        raise Refused('output_invalid')
    parts=output[1:].split('/')
    if len(parts)<2 or any(part in ('','.','..') for part in parts): raise Refused('output_invalid')
    rootfd=None; outfd=None; cursor=None
    try:
        rootfd=_open_absolute_directory(project_root)
        outfd=_open_absolute_directory('/'+'/'.join(parts[:-1]))
        root=os.fstat(rootfd); parent=os.fstat(outfd)
        if parent.st_uid != os.getuid() or stat.S_IMODE(parent.st_mode)&0o077:
            raise Refused('output_parent_not_private')
        cursor=os.dup(outfd)
        while True:
            current=os.fstat(cursor)
            if (current.st_dev,current.st_ino)==(root.st_dev,root.st_ino):
                raise Refused('output_inside_project')
            nextfd=os.open('..',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=cursor)
            ancestor=os.fstat(nextfd)
            if (ancestor.st_dev,ancestor.st_ino)==(current.st_dev,current.st_ino):
                os.close(nextfd); break
            os.close(cursor); cursor=nextfd
        result=outfd; outfd=None
        return result,parts[-1]
    except OSError as exc:
        raise Refused('output_or_project_unavailable') from exc
    finally:
        for fd in (cursor,outfd,rootfd):
            if fd is not None: os.close(fd)

def require_parent_within(parentfd, output_root):
    """Require an already-open parent to remain beneath an owned private root."""
    rootfd=None; cursor=None
    try:
        rootfd=_open_absolute_directory(output_root); root=os.fstat(rootfd)
        if root.st_uid!=os.getuid() or stat.S_IMODE(root.st_mode)&0o077:
            raise Refused('output_scope_not_private')
        cursor=os.dup(parentfd)
        while True:
            current=os.fstat(cursor)
            if (current.st_dev,current.st_ino)==(root.st_dev,root.st_ino): return
            nxt=os.open('..',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=cursor)
            parent=os.fstat(nxt)
            if (parent.st_dev,parent.st_ino)==(current.st_dev,current.st_ino):
                os.close(nxt); raise Refused('output_outside_owned_scope')
            os.close(cursor); cursor=nxt
    except OSError as exc:
        raise Refused('output_scope_unavailable') from exc
    finally:
        for fd in (cursor,rootfd):
            if fd is not None: os.close(fd)
