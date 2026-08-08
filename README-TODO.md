# Configuration

### Registry

Should have some kind of a registry of maps which will be sharead across scripts.

Use cases:
- auth regirsty
- secrets (including loading from env)
- some k-v data for lua scripts


# Optimization 

## RequestInput optimizations or not
Execution inside script require additional clone/+Arch reference.
Thus I can not mutate RequestInput via set_arg from script.

Initially It was possible to use Arc<HashMap> and return underlying values by reference.
Now it is Arch<Mutex<Map>>.
Access to RequestInput shold be sequential, read only for most cases.

So it is not clear should I optimize somthing here or not.

I was thinking about using ArcSwap instead but it could be overkill.
MutexGuard also does not look as a reasonable solution.
