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


# HINTS

Multiple data sources (this could be a killer feature)


The "Preflight" Difference (CORS)
When you make a request from a browser to a different domain (your API):
Authorization: Bearer: This is a standard header. If your API is configured for CORS, the browser will likely still trigger a "preflight" (OPTIONS) request, but many servers and gateways are pre-configured to handle Authorization automatically.
api-key or x-api-key: These are considered non-standard custom headers. When a browser sees a custom header, it always triggers a CORS preflight request.
The Trap: If your server-side CORS policy does not explicitly list api-key in its Access-Control-Allow-Headers list, the browser will block the request entirely. 
