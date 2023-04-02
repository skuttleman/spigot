# Spigot Core

This is the core of the spigot workflow library.

## Extend The API

See `spigot.impl.api` for some handy utilities for building your own custom workflow directives
(i.e. `:spigot/serial`). The API can be extended by implementing multimethods in `spigot.impl.multis`.
You can also implement your own value resolvers and reducers - like `(spigot/get ...)` and
`(spigot/each ...)` by extending the multimethods in `spigot.impl.context`.

## Tests

```bash
$ clj -A:dev -m kaocha.runner 
```
