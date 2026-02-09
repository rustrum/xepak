# TODO

- multiple data sources (this could be a killer feature)

# HINTS

The "Preflight" Difference (CORS)
When you make a request from a browser to a different domain (your API):
Authorization: Bearer: This is a standard header. If your API is configured for CORS, the browser will likely still trigger a "preflight" (OPTIONS) request, but many servers and gateways are pre-configured to handle Authorization automatically.
api-key or x-api-key: These are considered non-standard custom headers. When a browser sees a custom header, it always triggers a CORS preflight request.
The Trap: If your server-side CORS policy does not explicitly list api-key in its Access-Control-Allow-Headers list, the browser will block the request entirely. 
