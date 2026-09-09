# POST request
look for request_type="POST" it is exists in DSL but does not work now
I Guess I should add some type of request limitations or maybe not.
For example POS /user/ID should create-update user.
BUT GET /user/ID should just return user.
I do not want to have different endpoints for different request types.
Maybe I should rely on LUA for this specific case like GET/POST/PUT/DELETE at one URL
HINT what if I will just limit request types for endpoints (by default GET)
and if you need another one you must provide what type of request types allowed


# Optimization 

## Optimise RequestInput or not?
Execution inside script require additional clone/+Arch reference.
Thus I can not mutate RequestInput via set_arg from script.

Initially It was possible to use Arc<HashMap> and return underlying values by reference.
Now it is Arch<Mutex<Map>>.
Access to RequestInput shold be sequential, read only for most cases.

So it is not clear should I optimize somthing here or not.

I was thinking about using ArcSwap instead but it could be overkill.
MutexGuard also does not look as a reasonable solution.
