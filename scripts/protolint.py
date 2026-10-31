import re,sys
from google.protobuf import descriptor_pb2
fds=descriptor_pb2.FileDescriptorSet(); fds.ParseFromString(open(sys.argv[1],'rb').read())
errs=[]; n_msg=n_enum=n_field=0
snake=re.compile(r'^[a-z][a-z0-9]*(_[a-z0-9]+)*$'); pascal=re.compile(r'^[A-Z][A-Za-z0-9]*$')
def up(name): return re.sub(r'(?<!^)([A-Z])',r'_\1',name).upper()
def chk_enum(e,where):
    global n_enum; n_enum+=1
    pre=up(e.name)+'_'
    if not pascal.match(e.name): errs.append(f'{where}{e.name}: enum not PascalCase')
    if e.value[0].number!=0 or not e.value[0].name.endswith('_UNSPECIFIED'): errs.append(f'{where}{e.name}: zero value must be *_UNSPECIFIED')
    for v in e.value:
        if not v.name.startswith(pre): errs.append(f'{where}{e.name}.{v.name}: missing prefix {pre}')
def chk_msg(m,where):
    global n_msg,n_field; n_msg+=1
    if not pascal.match(m.name): errs.append(f'{where}{m.name}: message not PascalCase')
    nums=[f.number for f in m.field]
    if len(nums)!=len(set(nums)): errs.append(f'{where}{m.name}: duplicate field numbers')
    for f in m.field:
        n_field+=1
        if not snake.match(f.name): errs.append(f'{where}{m.name}.{f.name}: field not snake_case')
    for e in m.enum_type: chk_enum(e,where+m.name+'.')
    for nm in m.nested_type: chk_msg(nm,where+m.name+'.')
for fd in fds.file:
    if fd.package.startswith('google.'): continue
    if not re.match(r'^vortex\.[a-z]+\.v1$',fd.package): errs.append(f'{fd.name}: package {fd.package} violates vortex.<组件>.v1')
    if not fd.name.startswith(fd.package.replace('.','/')+'/'): errs.append(f'{fd.name}: path must match package')
    for m in fd.message_type: chk_msg(m,fd.package+'.')
    for e in fd.enum_type: chk_enum(e,fd.package+'.')
print(f'messages={n_msg} enums={n_enum} fields={n_field} errors={len(errs)}')
for e in errs: print(' -',e)
sys.exit(1 if errs else 0)
