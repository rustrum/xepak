# Next things to do

IMPORTANT limit/offset logic must be moved to pre-processors.
RequestInput must not be aware of such custom fields - it just holds and transfer data

# General thoughts

- Should I rename endpoint "resource"?
- Should add more input validators

# Blob type updates

Should probably wrap inner value in Arc to prevent cloning big Vec<u8>
Must think from the perspective of file uploads.

## File upload/serve functionality (next release)

- read data as blob.
- create lua function that writes bytes to file
- it must be in post-processor
    - resource will create record in DB and return it
    - LUA post processor will accept this value and will read/write file on disk
    - if LUA failed it could call DB and remove record
- retrieving data would require post processor to
    - reading record from DB or constructing local file path in LUA
    - it is passed to post-processor that reads data 
    - !!! or maybe it is a special post processor (without LUA) that accepts k-v map from resources and it could output file as stream from URL

Idea! Wrap input reader (to do not read all) instead of using blob. 


# DB named arg improvements (next release)

Should add support for nested fields and array indexes like `{{[0].name}}` if top level input is array.


# Sqlite Vector support (do later)

It is possible using sqlite-vec but unfortunately it is an experimental
library and it does not support indexing.
It is more reasonable to support PostgreSQL first.


# Optimization

## Request input

RequestInput is cloned into Lua so custom arguments now inside Arch<Mutex<Map>>.

Access to RequestInput should be sequential, read only for most cases.

So it is not clear should I optimize something here or not.

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
