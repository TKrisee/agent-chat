"""Durable per-identity change counters for read-only goal selection.

Absent or incomplete hooks are deliberately treated as legacy and retain the
conservative whole-database data_version fence. Installation is a writable
schema migration, never performed by the goal observation.
"""

# table: (key column, columns whose changes affect selection)
REGISTRY={
 'root_config':('singleton',('home_project','home_session','thread_id','host_id','mode')),
 'root_directory_owner':('singleton',('home_project','home_session')),
 'root_projects':('project_id',('session_id','orchestrator_session','joined')),
}
PROJECT={
 'meta':('key',('value',)),
 'sessions':('id',()),
 'project_orchestrators':('session_id',('root_session',)),
 'root_projections':('session_id',('home_project','home_session','thread_id')),
 'bridge_bindings':('session_id',('thread_id','parent_session','updated_at')),
 'remote_session_hosts':('session_id',('host_id',)),
 'bridge_dispatcher':('host_id',('owner','secret_hash','acquired_at')),
 'bridge_client_runtime':('host_id',('server','pid','error')),
 'session_resets':('target_session',('status',)),
}


def _trigger(table,key,columns,event):
    suffix={'INSERT':'ai','UPDATE':'au','DELETE':'ad'}[event]
    name='goal_generation_'+table+'_'+suffix
    if event=='UPDATE':
        condition=' OR '.join('OLD."%s" IS NOT NEW."%s"'%(c,c) for c in (key,*columns)) or '0'
        body=' '.join(_bump(table,side,key) for side in ('OLD','NEW'))
        when=' WHEN '+condition
    else:
        side='NEW' if event=='INSERT' else 'OLD'
        body=_bump(table,side,key);when=''
    return name, 'CREATE TRIGGER "%s" AFTER %s ON "%s"%s BEGIN %s END'%(name,event,table,when,body)


def _bump(table,side,key):
    return ("INSERT INTO goal_selection_generations(table_name,row_key,generation) VALUES('%s',CAST(%s.\"%s\" AS TEXT),1) "
            "ON CONFLICT(table_name,row_key) DO UPDATE SET generation=generation+1;"%(table,side,key))


def install(db, specification):
    existing={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'goal_selection_generations' not in existing:
        db.execute('CREATE TABLE IF NOT EXISTS goal_selection_generations(table_name TEXT NOT NULL,row_key TEXT NOT NULL,generation INTEGER NOT NULL,PRIMARY KEY(table_name,row_key))')
    installed={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'goal_generation_%'")}
    for table,(key,columns) in specification.items():
        if table not in existing: continue
        available={row[1] for row in db.execute('PRAGMA table_info("%s")'%table)}
        if not set((key,*columns)).issubset(available): continue
        for event in ('INSERT','UPDATE','DELETE'):
            name,sql=_trigger(table,key,columns,event)
            if name not in installed:
                db.execute(sql.replace('CREATE TRIGGER ', 'CREATE TRIGGER IF NOT EXISTS ',1))
    # Existing bad hooks must never become trusted as a valid migration.
    validate(db,specification,allow_incomplete=True)


def validate(db,specification,*,allow_incomplete=False):
    names={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'goal_selection_generations' not in names: return False
    triggers={row[0]:(row[1],row[2]) for row in db.execute(
        "SELECT name,tbl_name,sql FROM sqlite_master WHERE type='trigger' AND name LIKE 'goal_generation_%'")}
    for table,(key,columns) in specification.items():
        if table not in names:
            if allow_incomplete: continue
            return False
        available={row[1] for row in db.execute('PRAGMA table_info("%s")'%table)}
        if not set((key,*columns)).issubset(available):
            if allow_incomplete: continue
            return False
        for event in ('INSERT','UPDATE','DELETE'):
            name,expected=_trigger(table,key,columns,event)
            actual=triggers.get(name)
            if not actual:
                if allow_incomplete: continue
                return False
            norm=lambda value:' '.join(value.lower().replace(' if not exists ',' ').split())
            if actual[0]!=table or norm(actual[1])!=norm(expected): raise RuntimeError('goal selection generation hook changed')
    return True


def value(db,table,key):
    row=db.execute('SELECT generation FROM goal_selection_generations WHERE table_name=? AND row_key=?',(table,str(key))).fetchone()
    return row[0] if row else 0
