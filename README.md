<p align="center" width="100%" style="text-align:center">
<img src="./xepak-rest.png" alt="REST service for your DB" />
</p>

This is gonna be DSL based REST service for your DB. Something like PostgRest but not for PostgreSQL and written in Rust instead Haskell.

Right now I'm focusing on Sqlite as a main DB backend, because why not. Later will add all DB that are supported by sqlx library.


I hope it will be an interesting project for you. But there is a lot work needed to be done here. Please visit this project later in one or two months.


## TODO things

 - Authorize processor should use role expression string like:
   - "manager OR admin OR (office AND billing)" -> (or (or (name manager) (name admin)) (and (name office) (name billing)))
   - "manager || admin || (office && billing)"
   - "OR(manager admin AND(office billing))"


## Scripting consideration Rhai vs Lua ?

### Rhai pros

Already integrated in XepakREST, good Rust interop, similar syntax, more advanced type system that could be used to process storage outputs in script and not only.

### Lua pros

Well known syntax, could pass types in script by ref instead clone, support async, could be faster if jitted.

### Performance issues considerations ...

Now it is impossible to determine will Rhai be slower in real world scenarios or not.
There are many factors that would affect final performance expect script execution speed.
Need to wait for real Rhai usage examples in production
and only then considering Lua scripting feature and comparisons tests.


## License

This product distributed under MIT license BUT only under certain conditions that listed in the LICENSE-TERMS file.

I know it's kina silly but I'm not in the mood right now to write my own license. Will do it later.
