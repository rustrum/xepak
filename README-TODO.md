# Input validators

I guess it does not work now. Should enable and add some tests to it.

Should later add ability to have a nested N layer data in request body.
Now it is just forced to fail if it is more than just a K/V dict.


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

## CORS processing

Not implemented right now.
**Do not want to do it right now** - proxy server or API gateway that handle HTTPS should be responsible for it.
!!! TODO: Add documentation to emphasize this.


The "Preflight" Difference (CORS)
When you make a request from a browser to a different domain (your API):

Authorization:Bearer:
This is a standard header. If your API is configured for CORS, the browser will likely still trigger a "preflight" (OPTIONS) request, but many servers and gateways are pre-configured to handle Authorization automatically.

api-key or x-api-key:
These are considered non-standard custom headers. When a browser sees a custom header, it always triggers a CORS preflight request.

The Trap: If your server-side CORS policy does not explicitly list api-key in its Access-Control-Allow-Headers list, the browser will block the request entirely. 
