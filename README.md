<p align="center" width="100%" style="text-align:center">
<img src="./xepak-rest.png" alt="REST service for your DB" />
</p>

This is gonna be DSL based REST service for your DB. Something like PostgRest but not for PostgreSQL and written in Rust instead Haskell.

Right now I'm focusing on Sqlite as a main DB backend, because why not. Later will add all DB that are supported by sqlx library.


I hope it will be an interesting project for you. But there is a lot work needed to be done here. Please visit this project later in one or two months.

##

## Scripting support

Right now Xepak supports two scripting languages.

- **LUA** is the main scripting language.
- **Rhai** is an optional scripting lanuage.

### Performance considerations

Now it is impossible to determine will Rhai be slower in real world scenarios or not.
There are many factors that would affect final performance expect script execution speed.
But if LUA will outperform Rhai in tests then Rhai will be dismissed.


## License

This product distributed under MIT license BUT only under certain conditions that listed in the LICENSE-TERMS file.

I know it's kina silly but I'm not in the mood right now to write my own license. Will do it later.
