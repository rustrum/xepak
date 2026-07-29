# Optimization 

## RequestInput
I do not like idea of mutex inside it but I must mutate it's inner state inside script.
Probably I would require RequestInput after script execution for post processing.
Maybe I can consume RequestInput while executing script and then return updated version?
Need to test peformance first with heavy usage in scripting.
